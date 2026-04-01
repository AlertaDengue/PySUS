import asyncio
import gzip
import shutil
import tarfile
import zipfile
import csv
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional, Type, Union

import anyio
import chardet
import fastparquet
import magic
import pandas as pd

from pysus import CACHEPATH
from pysus.api.models import BaseCompressedFile, BaseLocalFile, BaseTabularFile


class File(BaseLocalFile):
    type: str = None

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
    extension: str = ""
    type: str = "DIR"

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
    type: str = "CSV"
    _encoding: Optional[str] = None
    _sep: Optional[str] = None

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
    type: str = "Parquet"

    async def load(self) -> pd.DataFrame:
        return await anyio.to_thread.run_sync(pd.read_parquet, self.path)

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


class DBF(BaseLocalFile, BaseTabularFile):
    type: str = "DBF"

    async def load(self) -> pd.DataFrame:
        from dbfread import DBF

        def _load():
            return pd.DataFrame(iter(DBF(self.path)))

        return await anyio.to_thread.run_sync(_load)

    async def stream(
        self,
        chunk_size: int = 10000,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        from dbfread import DBF

        dbf_file = await anyio.to_thread.run_sync(DBF, self.path)
        records = []

        for i, record in enumerate(dbf_file):
            records.append(record)
            if (i + 1) % chunk_size == 0:
                yield pd.DataFrame(records)
                records = []
                await anyio.sleep(0)

        if records:
            yield pd.DataFrame(records)


class DBC(BaseLocalFile, BaseTabularFile):
    type: str = "DBC"

    async def load(self) -> pd.DataFrame:
        from pyreaddbc import read_dbc

        return await anyio.to_thread.run_sync(read_dbc, str(self.path))

    async def stream(
        self,
        chunk_size: int = 10000,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        yield await self.load()


class JSON(BaseLocalFile, BaseTabularFile):
    type: str = "JSON"

    async def load(self) -> pd.DataFrame:
        return await anyio.to_thread.run_sync(pd.read_json, self.path)

    async def stream(
        self,
        chunk_size: Optional[int] = None,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        yield await self.load()


class PDF(BaseLocalFile):
    type: str = "PDF"

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
    type: str = "ZIP"

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
        import asyncio
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
    type: str = "ZIP"

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
    type: str = "ZIP"

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
        import asyncio
        from pysus.api.extensions import ExtensionFactory

        target_dir.mkdir(parents=True, exist_ok=True)
        members = await self.list_members()

        def _extract():
            with tarfile.open(self.path) as t:
                t.extractall(target_dir)

        await anyio.to_thread.run_sync(_extract)

        tasks = [ExtensionFactory.instantiate(target_dir / m) for m in members]
        return list(await asyncio.gather(*tasks))


class ExtensionFactory:
    _mime: Dict[str, Type[BaseLocalFile]] = {
        "application/zip": Zip,
        "application/x-gzip": GZip,
        "application/x-tar": Tar,
        "text/csv": CSV,
        "application/pdf": PDF,
        "application/json": JSON,
        "application/x-dbf": DBF,
    }

    _extensions: Dict[str, Type[BaseLocalFile]] = {
        ".zip": Zip,
        ".gz": GZip,
        ".tar": Tar,
        ".tgz": Tar,
        ".tar.gz": Tar,
        ".csv": CSV,
        ".parquet": Parquet,
        ".dbf": DBF,
        ".dbc": DBC,
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
            return Directory(basename=path.name, path=path, extension="")

        FileClass = await cls.get_file_class(path)

        return FileClass(
            basename=path.name,
            path=path,
            extension="".join(path.suffixes).lower() or path.suffix.lower(),
        )
