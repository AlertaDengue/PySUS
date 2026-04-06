import asyncio
import csv
import gzip
import shutil
import tarfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional, Type, Union

import anyio
import chardet
import fastparquet
import magic
import pandas as pd
import pyarrow.parquet as pq
from pydantic import Field
from pysus import CACHEPATH
from pysus.api.models import BaseCompressedFile, BaseLocalFile, BaseTabularFile

from .types import FileType

try:
    from dbfread import DBF as DBFReader
    from pyreaddbc import dbc2dbf, read_dbc

    FTP_IMPORT = True
except ImportError:
    FTP_IMPORT = False


class File(BaseLocalFile):
    type: FileType = Field(None)

    async def load(self) -> bytes:
        return await anyio.to_thread.run_sync(self.path.read_bytes)

    async def stream(
        self,
        chunk_size: int = 1024 * 1024,
    ) -> AsyncGenerator[bytes, None]:
        def _read_sync():
            with open(self.path, "rb") as f:
                while chunk := f.read(chunk_size):
                    yield chunk

        for chunk in _read_sync():
            yield chunk
            await anyio.sleep(0)


class Directory(BaseLocalFile):
    type: FileType = Field("DIR")

    def __repr__(self) -> str:
        return f"{self.basename}/"

    async def load(self) -> List[BaseLocalFile]:
        from pysus.api.extensions import ExtensionFactory

        if not self.path.exists():
            return []

        paths = list(self.path.iterdir())

        tasks = [ExtensionFactory.instantiate(p) for p in paths]

        return list(await asyncio.gather(*tasks))

    async def stream(
        self,
        chunk_size: Optional[int] = None,
    ) -> AsyncGenerator[BaseLocalFile, None]:
        from pysus.api.extensions import ExtensionFactory

        for p in self.path.iterdir():
            yield await ExtensionFactory.instantiate(p)


class CSV(BaseLocalFile, BaseTabularFile):
    type: FileType = Field("CSV")
    _encoding: Optional[str] = None
    _sep: Optional[str] = None

    @property
    def columns(self) -> List[str]:
        separator = asyncio.run(self._get_sep())
        encoding = asyncio.run(self._get_encoding())
        df = pd.read_csv(self.path, sep=separator, encoding=encoding, nrows=0)
        return df.columns.tolist()

    @property
    def rows(self) -> int:
        count = 0
        with open(self.path, "rb") as f:
            for line in f:
                count += 1
        return count - 1

    async def _get_encoding(self) -> str:
        if self._encoding is None:

            def detect():
                with open(self.path, "rb") as f:
                    return chardet.detect(f.read(1024 * 300))

            result = await anyio.to_thread.run_sync(detect)
            self._encoding = result["encoding"] or "utf-8"
        return self._encoding

    async def _get_sep(self) -> str:
        if self._sep is None:
            encoding = await self._get_encoding()

            def sniff():
                try:
                    with open(self.path, "r", encoding=encoding) as f:
                        sample = f.read(1024 * 10)
                        dialect = csv.Sniffer().sniff(sample)
                        return dialect.delimiter
                except Exception:
                    return ","

            self._sep = await anyio.to_thread.run_sync(sniff)
        return self._sep

    async def load(self) -> pd.DataFrame:
        encoding = await self._get_encoding()
        separator = await self._get_sep()

        def _read():
            return pd.read_csv(self.path, sep=separator, encoding=encoding)

        return await anyio.to_thread.run_sync(_read)

    async def stream(
        self,
        chunk_size: int = 10000,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        encoding = await self._get_encoding()
        separator = await self._get_sep()

        def _get_reader():
            return pd.read_csv(
                self.path,
                sep=separator,
                encoding=encoding,
                chunksize=chunk_size,
                dtype=str,
                low_memory=False,
            )

        reader = await anyio.to_thread.run_sync(_get_reader)

        for chunk in reader:
            yield chunk
            await anyio.sleep(0)


class Parquet(BaseLocalFile, BaseTabularFile):
    type: FileType = Field("Parquet")

    @property
    def columns(self) -> List[str]:
        return pq.read_schema(self.path).names

    @property
    def rows(self) -> int:
        return pq.read_metadata(self.path).num_rows

    async def load(self, parse: bool = True) -> pd.DataFrame:
        def _load():
            df = pd.read_parquet(self.path)
            if parse:
                df = self.parse_dftypes(df)
            return df

        return await anyio.to_thread.run_sync(_load)

    async def stream(
        self,
        chunk_size: int = 10000,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        pf = await anyio.to_thread.run_sync(
            fastparquet.ParquetFile,
            str(self.path),
        )
        for batch in pf.iter_row_groups():
            yield batch
            await anyio.sleep(0)

    async def to_parquet(
        self, output_path: Optional[Union[str, Path]] = None, **kwargs
    ) -> "Parquet":
        from pysus.api.extensions import ExtensionFactory

        if output_path is None or Path(output_path) == self.path:
            return self

        await anyio.to_thread.run_sync(shutil.copy, self.path, output_path)
        return await ExtensionFactory.instantiate(output_path)

    @staticmethod
    def parse_dftypes(df: pd.DataFrame) -> pd.DataFrame:
        def map_column_func(column_names: list[str], func):
            columns = [c for c in df.columns if c in column_names]
            if columns:
                df[columns] = df[columns].map(func)

        def str_to_int(string: str):
            clean = str(string).replace(" ", "")
            if clean.isnumeric():
                return int(clean)
            return string

        def str_to_date(string: str):
            if isinstance(string, str):
                try:
                    return datetime.strptime(string, "%Y%m%d").date()
                except ValueError:
                    return string
            return string

        map_column_func(["DT_NOTIFIC", "DT_SIN_PRI"], str_to_date)
        map_column_func(["CODMUNRES", "SEXO"], str_to_int)

        df = df.map(lambda x: "" if str(x).isspace() else x)
        return df.convert_dtypes()


class DBF(BaseLocalFile, BaseTabularFile):
    type: FileType = Field("DBF")

    @property
    def columns(self) -> List[str]:
        return DBFReader(self.path, load=False).field_names

    @property
    def rows(self) -> int:
        return len(DBFReader(self.path, load=False))

    def decode_column(self, value):
        if isinstance(value, bytes):
            return value.decode(encoding="iso-8859-1").replace("\x00", "")
        if isinstance(value, str):
            return str(value).replace("\x00", "")
        return value

    async def load(self) -> pd.DataFrame:
        def _load():
            dbf = DBFReader(self.path, encoding="iso-8859-1", raw=True)
            df = pd.DataFrame(iter(dbf))
            return df.map(self.decode_column)

        return await anyio.to_thread.run_sync(_load)

    async def stream(
        self,
        chunk_size: int = 30000,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        def _get_db():
            return DBFReader(self.path, encoding="iso-8859-1", raw=True)

        dbf_file = await anyio.to_thread.run_sync(_get_db)
        records = []

        for i, record in enumerate(dbf_file):
            records.append(record)
            if (i + 1) % chunk_size == 0:
                df = pd.DataFrame(records).map(self.decode_column)
                yield df
                records = []
                await anyio.sleep(0)

        if records:
            yield pd.DataFrame(records).map(self.decode_column)


class DBC(BaseLocalFile, BaseTabularFile):
    type: FileType = Field("DBC")

    @property
    def columns(self) -> List[str]:
        df = asyncio.run(self.load())
        return df.columns.tolist()

    @property
    def rows(self) -> int:
        df = asyncio.run(self.load())
        return len(df)

    async def load(self) -> pd.DataFrame:
        return await anyio.to_thread.run_sync(read_dbc, str(self.path))

    async def stream(
        self,
        chunk_size: int = 10000,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        yield await self.load()

    async def to_parquet(
        self,
        output_path: Optional[Union[str, Path]] = None,
        chunk_size: int = 10000,
    ) -> "BaseTabularFile":
        from pysus.api.extensions import ExtensionFactory

        if output_path is None:
            output_path = self.path.with_suffix(".parquet")

        output_path = Path(output_path).expanduser().resolve()
        tmp_dbf = self.path.with_suffix(".dbf")

        if not tmp_dbf.exists():
            await anyio.to_thread.run_sync(
                dbc2dbf,
                str(self.path),
                str(tmp_dbf),
            )

        dbf = await ExtensionFactory.instantiate(tmp_dbf)

        try:
            parquet = await dbf.to_parquet(
                output_path=output_path,
                chunk_size=chunk_size,
            )
        finally:
            if tmp_dbf.exists():
                await anyio.to_thread.run_sync(tmp_dbf.unlink)

        return parquet


class JSON(BaseLocalFile, BaseTabularFile):
    type: FileType = Field("JSON")

    @property
    def columns(self) -> List[str]:
        df = (
            pd.read_json(self.path, nrows=0)
            if self.path.stat().st_size > 0
            else pd.DataFrame()
        )
        return df.columns.tolist()

    @property
    def rows(self) -> int:
        df = asyncio.run(self.load())
        return len(df)

    async def load(self) -> pd.DataFrame:
        return await anyio.to_thread.run_sync(pd.read_json, self.path)

    async def stream(
        self,
        chunk_size: Optional[int] = None,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        yield await self.load()


class PDF(BaseLocalFile):
    type: FileType = Field("PDF")

    async def load(self) -> bytes:
        return await anyio.to_thread.run_sync(self.path.read_bytes)

    async def stream(
        self,
        chunk_size: Optional[int] = None,
    ) -> AsyncGenerator[bytes, None]:
        def _read():
            with open(self.path, "rb") as f:
                if chunk_size:
                    while chunk := f.read(chunk_size):
                        yield chunk
                else:
                    yield f.read()

        for chunk in _read():
            yield chunk
            await anyio.sleep(0)


class Zip(BaseCompressedFile):
    type: FileType = Field("ZIP")

    async def load(self) -> zipfile.ZipFile:
        return await anyio.to_thread.run_sync(zipfile.ZipFile, self.path)

    async def list_members(self) -> List[str]:
        def _list():
            with zipfile.ZipFile(self.path) as z:
                return z.namelist()

        return await anyio.to_thread.run_sync(_list)

    async def open_member(self, member_name: str) -> bytes:
        def _open():
            with zipfile.ZipFile(self.path) as z:
                return z.read(member_name)

        return await anyio.to_thread.run_sync(_open)

    async def extract(
        self,
        target_dir: Optional[Path] = CACHEPATH,
    ) -> List[BaseLocalFile]:
        from pysus.api.extensions import ExtensionFactory

        target_dir.mkdir(parents=True, exist_ok=True)
        members = await self.list_members()

        def _extract_all():
            with zipfile.ZipFile(self.path) as z:
                z.extractall(target_dir)

        await anyio.to_thread.run_sync(_extract_all)

        tasks = [ExtensionFactory.instantiate(target_dir / m) for m in members]
        return list(await asyncio.gather(*tasks))


class GZip(BaseCompressedFile):
    type: FileType = Field("ZIP")

    async def load(self) -> bytes:
        def _read():
            with gzip.open(self.path, "rb") as f:
                return f.read()

        return await anyio.to_thread.run_sync(_read)

    async def list_members(self) -> List[str]:
        return [self.path.stem]

    async def open_member(self, member_name: str) -> bytes:
        return await self.load()

    async def extract(
        self,
        target_dir: Optional[Path] = CACHEPATH,
    ) -> List[BaseLocalFile]:
        from pysus.api.extensions import ExtensionFactory

        target_dir.mkdir(parents=True, exist_ok=True)
        out_file = target_dir / self.path.stem

        def _decompress():
            with gzip.open(self.path, "rb") as f_in:
                with open(out_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

        await anyio.to_thread.run_sync(_decompress)
        return [await ExtensionFactory.instantiate(out_file)]


class Tar(BaseCompressedFile):
    type: FileType = Field("ZIP")

    async def load(self) -> tarfile.TarFile:
        return await anyio.to_thread.run_sync(tarfile.open, self.path)

    async def list_members(self) -> List[str]:
        def _list():
            with tarfile.open(self.path) as t:
                return t.getnames()

        return await anyio.to_thread.run_sync(_list)

    async def open_member(self, member_name: str) -> bytes:
        def _open():
            with tarfile.open(self.path) as t:
                f = t.extractfile(member_name)
                return f.read() if f else b""

        return await anyio.to_thread.run_sync(_open)

    async def extract(
        self,
        target_dir: Optional[Path] = CACHEPATH,
    ) -> List[BaseLocalFile]:
        from pysus.api.extensions import ExtensionFactory

        target_dir.mkdir(parents=True, exist_ok=True)
        members = await self.list_members()

        def _extract():
            with tarfile.open(self.path) as t:
                t.extractall(target_dir)

        await anyio.to_thread.run_sync(_extract)

        tasks = [ExtensionFactory.instantiate(target_dir / m) for m in members]
        return list(await asyncio.gather(*tasks))


class FTPNotImported(BaseTabularFile):
    type: FileType = Field(None)
    import_err = """
        run "pip install pysus[ftp]" to handle DBC or DBF files


        NOTE:
        PySUS FTP api also requires a system dependency named 'libffi-dev'.
        If you are on windows, you may want to use the docker version of PySUS
        instead.
    """

    @property
    def columns(self) -> List[str]:
        raise ImportError(self.import_err)

    @property
    def rows(self) -> int:
        raise ImportError(self.import_err)

    async def load(self):
        raise ImportError(self.import_err)

    async def stream(self, chunk_size=None):
        raise ImportError(self.import_err)

    async def to_parquet(self, **kwargs):
        raise ImportError(self.import_err)


class ExtensionFactory:
    _mime: Dict[str, Type[BaseLocalFile]] = {
        "application/zip": Zip,
        "application/x-gzip": GZip,
        "application/x-tar": Tar,
        "text/csv": CSV,
        "application/pdf": PDF,
        "application/json": JSON,
        "application/x-dbf": DBF if FTP_IMPORT else FTPNotImported,
    }

    _extensions: Dict[str, Type[BaseLocalFile]] = {
        ".zip": Zip,
        ".gz": GZip,
        ".tar": Tar,
        ".tgz": Tar,
        ".tar.gz": Tar,
        ".csv": CSV,
        ".parquet": Parquet,
        ".dbf": DBF if FTP_IMPORT else FTPNotImported,
        ".dbc": DBC if FTP_IMPORT else FTPNotImported,
        ".pdf": PDF,
        ".json": JSON,
    }

    @classmethod
    async def _identify(cls, path: Path) -> Optional[Type[BaseLocalFile]]:
        try:
            mime = await anyio.to_thread.run_sync(
                magic.from_file,
                str(path),
                True,
            )
            return cls._mime.get(mime)
        except (ImportError, Exception):
            return None

    @classmethod
    async def get_file_class(cls, path: Path) -> Type[BaseLocalFile]:
        mime_class = await cls._identify(path)

        if mime_class:
            return mime_class

        extension = "".join(path.suffixes).lower()

        if extension in cls._extensions:
            return cls._extensions[extension]

        return cls._extensions.get(path.suffix.lower(), File)

    @classmethod
    async def instantiate(cls, path: Union[str, Path]) -> BaseLocalFile:
        path = Path(path).expanduser().resolve()

        is_directory = await anyio.to_thread.run_sync(path.is_dir)

        if is_directory:
            return Directory(path=path)

        FileClass = await cls.get_file_class(path)

        return FileClass(
            path=path,
        )
