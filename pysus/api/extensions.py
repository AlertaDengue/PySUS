import asyncio
import csv
import gzip
import shutil
import tarfile
import zipfile
from collections.abc import AsyncGenerator
from datetime import datetime
from pathlib import Path
from typing import ClassVar
from collections.abc import Callable

import anyio
import chardet
import fastparquet
import magic
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import Field, PrivateAttr
from dbfread import DBF as DBFReader

from pysus import CACHEPATH
from pysus.api.models import BaseCompressedFile, BaseLocalFile, BaseTabularFile

from .types import FileType

import sys
import ctypes.util

try:
    LIBFFI = True
    if sys.platform.startswith("linux"):
        LIBFFI = ctypes.util.find_library("ffi") is not None

    if LIBFFI:
        from pyreaddbc import dbc2dbf

        DBC_IMPORT = True
    else:
        DBC_IMPORT = False
except ImportError:
    DBC_IMPORT = False


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

    async def load(self) -> list[BaseLocalFile]:
        from pysus.api.extensions import ExtensionFactory

        if not self.path.exists():
            return []

        paths = list(self.path.iterdir())
        tasks = [ExtensionFactory.instantiate(p) for p in paths]
        return list(await asyncio.gather(*tasks))

    async def stream(
        self,
        chunk_size: int | None = None,
    ) -> AsyncGenerator[BaseLocalFile, None]:
        from pysus.api.extensions import ExtensionFactory

        for p in self.path.iterdir():
            yield await ExtensionFactory.instantiate(p)


class CSV(BaseTabularFile):
    type: FileType = Field("CSV")
    _encoding: str | None = PrivateAttr(default=None)
    _sep: str | None = PrivateAttr(default=None)

    @property
    def columns(self) -> list[str]:
        df = pd.read_csv(self.path, sep=",", nrows=0)
        return df.columns.tolist()

    @property
    def rows(self) -> int:
        count = 0
        with open(self.path, "rb") as f:
            for _ in f:
                count += 1
        return max(0, count - 1)

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
                    with open(self.path, encoding=encoding) as f:
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

        def _read_sync():
            return pd.read_csv(
                self.path, sep=separator, encoding=encoding, low_memory=False
            )

        return await anyio.to_thread.run_sync(_read_sync)

    async def stream(
        self,
        chunk_size: int = 10000,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        encoding = await self._get_encoding()
        separator = await self._get_sep()

        def _get_reader_sync():
            return pd.read_csv(
                self.path,
                sep=separator,
                encoding=encoding,
                chunksize=chunk_size,
                dtype=str,
                low_memory=False,
            )

        reader = await anyio.to_thread.run_sync(_get_reader_sync)
        for chunk in reader:
            yield chunk
            await anyio.sleep(0)


class Parquet(BaseTabularFile):
    type: FileType = Field("Parquet")

    @property
    def schema(self) -> pa.Schema:
        return pq.read_schema(self.path)

    @property
    def columns(self) -> list[str]:
        return pq.read_schema(self.path).names

    @property
    def rows(self) -> int:
        return pq.read_metadata(self.path).num_rows

    async def load(self, parse: bool = True) -> pd.DataFrame:
        def _load():
            df = pd.read_parquet(self.path)
            return self.parse_dftypes(df) if parse else df

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

    @staticmethod
    def parse_dftypes(df: pd.DataFrame) -> pd.DataFrame:
        def str_to_int(string):
            clean = str(string).replace(" ", "")
            return int(clean) if clean.isnumeric() else string

        def str_to_date(string):
            if isinstance(string, str):
                try:
                    return datetime.strptime(string, "%Y%m%d").date()
                except ValueError:
                    return string
            return string

        cols_to_date = ["DT_NOTIFIC", "DT_SIN_PRI", "DT_NASC", "DT_INTER"]
        cols_to_int = ["CODMUNRES", "SEXO", "IDADE"]

        for col in df.columns:
            if col in cols_to_date:
                df[col] = df[col].map(str_to_date)
            elif col in cols_to_int:
                df[col] = df[col].map(str_to_int)

        df = df.map(lambda x: "" if str(x).isspace() else x)
        return df.convert_dtypes()


class DBF(BaseTabularFile):
    type: FileType = Field("DBF")

    @property
    def columns(self) -> list[str]:
        return DBFReader(self.path, load=False).field_names

    @property
    def rows(self) -> int:
        return len(DBFReader(self.path, load=False))

    def decode_column(self, value):
        if isinstance(value, bytes):
            return (
                value.decode(encoding="cp1252", errors="replace")
                .replace("\x00", "")
                .strip()
            )
        if isinstance(value, str):
            return value.replace("\x00", "").strip()
        return value

    async def load(self) -> pd.DataFrame:
        def _load():
            dbf = DBFReader(self.path, encoding="cp1252", raw=True)
            df = pd.DataFrame(iter(dbf))
            return df.map(self.decode_column)

        return await anyio.to_thread.run_sync(_load)

    async def stream(
        self,
        chunk_size: int = 30000,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        def _get_db():
            return DBFReader(self.path, encoding="cp1252", raw=True)

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

    async def to_parquet(
        self,
        output_path: str | Path | None = None,
        chunk_size: int = 30000,
        callback: Callable[[int, int], None] = None,
    ) -> "Parquet":
        from pysus.api.extensions import ExtensionFactory

        out = (
            Path(output_path or self.path.with_suffix(".parquet"))
            .expanduser()
            .resolve()
        )

        if out.exists():
            return await ExtensionFactory.instantiate(out)

        async def _stream_to_single_file():
            dbf_reader = DBFReader(self.path, encoding="cp1252", raw=True)
            total_rows = len(dbf_reader)
            writer = None
            records = []

            try:
                for i, record in enumerate(dbf_reader):
                    records.append(record)
                    current_count = i + 1

                    if current_count % chunk_size == 0:
                        df = pd.DataFrame(records).map(self.decode_column)
                        table = pa.Table.from_pandas(df)
                        if writer is None:
                            writer = pq.ParquetWriter(str(out), table.schema)
                        writer.write_table(table)
                        records = []

                        if callback:
                            callback(current_count, total_rows)
                        await anyio.sleep(0)

                if records:
                    df = pd.DataFrame(records).map(self.decode_column)
                    table = pa.Table.from_pandas(df)
                    if writer is None:
                        writer = pq.ParquetWriter(str(out), table.schema)
                    writer.write_table(table)

                    if callback:
                        callback(total_rows, total_rows)

                if writer is None:
                    df_empty = pd.DataFrame(columns=self.columns)
                    table_empty = pa.Table.from_pandas(df_empty)
                    writer = pq.ParquetWriter(str(out), table_empty.schema)

            finally:
                if writer:
                    writer.close()

        await _stream_to_single_file()
        return await ExtensionFactory.instantiate(out)


class DBC(BaseTabularFile):
    type: FileType = Field("DBC")

    @property
    def columns(self) -> list[str]:
        raise NotImplementedError(
            "DBC metadata cannot be read directly. Convert to Parquet first."
        )

    @property
    def rows(self) -> int:
        raise NotImplementedError(
            "DBC metadata cannot be read directly. Convert to Parquet first."
        )

    async def load(self) -> pd.DataFrame:
        parquet = await self.to_parquet()
        return await parquet.load()

    async def stream(
        self,
        chunk_size: int = 10000,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        parquet = await self.to_parquet()
        async for chunk in parquet.stream(chunk_size=chunk_size):
            yield chunk

    async def to_parquet(
        self,
        output_path: str | Path | None = None,
        chunk_size: int = 30000,
        callback: Callable[[int, int], None] = None,
    ) -> "Parquet":
        from pysus.api.extensions import ExtensionFactory

        if output_path is None:
            output_path = self.path.with_suffix(".parquet")

        output_path = Path(output_path).expanduser().resolve()
        if output_path.exists():
            return await ExtensionFactory.instantiate(output_path)

        tmp_dbf_path = self.path.with_suffix(".dbf")
        try:
            await anyio.to_thread.run_sync(
                dbc2dbf,
                str(self.path),
                str(tmp_dbf_path),
            )
            dbf_ext = await ExtensionFactory.instantiate(tmp_dbf_path)
            return await dbf_ext.to_parquet(
                output_path=output_path,
                chunk_size=chunk_size,
                callback=callback,
            )
        finally:
            if tmp_dbf_path.exists():
                await anyio.to_thread.run_sync(tmp_dbf_path.unlink)


class JSON(BaseTabularFile):
    type: FileType = Field("JSON")

    @property
    def columns(self) -> list[str]:
        df = (
            pd.read_json(self.path, nrows=0)
            if self.path.stat().st_size > 0
            else pd.DataFrame()
        )
        return df.columns.tolist()

    @property
    def rows(self) -> int:
        return len(pd.read_json(self.path))

    async def load(self) -> pd.DataFrame:
        return await anyio.to_thread.run_sync(pd.read_json, self.path)

    async def stream(
        self, chunk_size: int | None = None
    ) -> AsyncGenerator[pd.DataFrame, None]:
        yield await self.load()


class PDF(BaseLocalFile):
    type: FileType = Field("PDF")

    async def load(self) -> bytes:
        return await anyio.to_thread.run_sync(self.path.read_bytes)

    async def stream(
        self, chunk_size: int | None = None
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

    async def list_members(self) -> list[str]:
        def _list():
            with zipfile.ZipFile(self.path) as z:
                return z.namelist()

        return await anyio.to_thread.run_sync(_list)

    async def open_member(self, member_name: str) -> bytes:
        def _read():
            with zipfile.ZipFile(self.path) as z:
                return z.read(member_name)

        return await anyio.to_thread.run_sync(_read)

    async def extract(
        self,
        target_dir: Path = CACHEPATH,
    ) -> list[BaseLocalFile]:
        from pysus.api.extensions import ExtensionFactory

        target_dir = Path(target_dir).expanduser().resolve()
        target_dir.mkdir(parents=True, exist_ok=True)

        def _extract_sync():
            with zipfile.ZipFile(self.path) as z:
                z.extractall(target_dir)

        await anyio.to_thread.run_sync(_extract_sync)

        members = await self.list_members()
        tasks = [ExtensionFactory.instantiate(target_dir / m) for m in members]
        return list(await asyncio.gather(*tasks))

    async def to_parquet(
        self,
        output_path: str | Path | None = None,
        chunk_size: int = 30000,
        callback: Callable[[int, int], None] | None = None,
    ) -> "Parquet":
        final_output = (
            Path(output_path or self.path.with_suffix(".parquet"))
            .expanduser()
            .resolve()
        )
        temp_dir = self.path.with_suffix(".tmp_extract")

        try:
            extracted_files = await self.extract(target_dir=temp_dir)

            tabular_file = next(
                (f for f in extracted_files if isinstance(f, BaseTabularFile)),
                None,
            )

            if not tabular_file:
                raise ValueError(
                    f"No tabular file found inside {self.path.name}",
                )

            return await tabular_file.to_parquet(
                output_path=final_output,
                chunk_size=chunk_size,
                callback=callback,
            )
        finally:
            await self._safe_cleanup(temp_dir)

    async def _safe_cleanup(self, directory: Path):
        def _cleanup():
            if not directory.exists():
                return

            for item in directory.iterdir():
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    for subitem in item.iterdir():
                        if subitem.is_file():
                            subitem.unlink()
                    item.rmdir()

            if directory.exists():
                directory.rmdir()

        await anyio.to_thread.run_sync(_cleanup)


class GZip(BaseCompressedFile):
    type: FileType = Field("ZIP")

    async def load(self) -> bytes:
        def _read():
            with gzip.open(self.path, "rb") as f:
                return f.read()

        return await anyio.to_thread.run_sync(_read)

    async def list_members(self) -> list[str]:
        return [self.path.stem]

    async def open_member(self, member_name: str) -> bytes:
        return await self.load()

    async def extract(
        self,
        target_dir: Path = CACHEPATH,
    ) -> list[BaseLocalFile]:
        from pysus.api.extensions import ExtensionFactory

        target_dir.mkdir(parents=True, exist_ok=True)
        out_file = target_dir / self.path.stem

        def _decompress():
            with (
                gzip.open(self.path, "rb") as f_in,
                open(
                    out_file,
                    "wb",
                ) as f_out,
            ):
                shutil.copyfileobj(f_in, f_out)

        await anyio.to_thread.run_sync(_decompress)
        return [await ExtensionFactory.instantiate(out_file)]


class Tar(BaseCompressedFile):
    type: FileType = Field("ZIP")

    async def load(self) -> tarfile.TarFile:
        return await anyio.to_thread.run_sync(tarfile.open, self.path)

    async def list_members(self) -> list[str]:
        def _list():
            with tarfile.open(self.path) as t:
                return t.getnames()

        return await anyio.to_thread.run_sync(_list)

    async def open_member(self, member_name: str) -> bytes:
        def _read():
            with tarfile.open(self.path) as t:
                f = t.extractfile(member_name)
                return f.read() if f else b""

        return await anyio.to_thread.run_sync(_read)

    async def extract(
        self,
        target_dir: Path = CACHEPATH,
    ) -> list[BaseLocalFile]:
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
    import_err: ClassVar[str] = """
        run "pip install pysus[dbc]" to handle DBC files.
        Make sure you also have libffi installed on the system. It may not work
        on Windows
    """

    @property
    def columns(self) -> list[str]:
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
    _mime: dict[str, type[BaseLocalFile]] = {
        "application/zip": Zip,
        "application/x-gzip": GZip,
        "application/x-tar": Tar,
        "text/csv": CSV,
        "application/pdf": PDF,
        "application/json": JSON,
    }

    _extensions: dict[str, type[BaseLocalFile]] = {
        ".zip": Zip,
        ".gz": GZip,
        ".tar": Tar,
        ".tgz": Tar,
        ".tar.gz": Tar,
        ".csv": CSV,
        ".parquet": Parquet,
        ".dbf": DBF,
        ".dbc": DBC if DBC_IMPORT else FTPNotImported,
        ".pdf": PDF,
        ".json": JSON,
    }

    @classmethod
    async def _identify(cls, path: Path) -> type[BaseLocalFile] | None:
        try:
            mime = await anyio.to_thread.run_sync(
                magic.from_file,
                str(path),
                True,
            )
            return cls._mime.get(mime)
        except Exception:
            return None

    @classmethod
    async def get_file_class(cls, path: Path) -> type[BaseLocalFile]:
        mime_class = await cls._identify(path)
        if mime_class:
            return mime_class
        extension = "".join(path.suffixes).lower()
        if extension in cls._extensions:
            return cls._extensions[extension]
        return cls._extensions.get(path.suffix.lower(), File)

    @classmethod
    async def instantiate(cls, path: str | Path) -> BaseLocalFile:
        path = Path(path).expanduser().resolve()
        if await anyio.to_thread.run_sync(path.is_dir):
            return Directory(path=path)
        FileClass = await cls.get_file_class(path)
        return FileClass(path=path)
