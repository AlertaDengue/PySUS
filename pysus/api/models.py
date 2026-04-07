from __future__ import annotations

import asyncio
import hashlib
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, List, Optional, Union

import anyio
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from pysus import CACHEPATH
from tqdm.asyncio import tqdm

from .types import State


class BaseFile(BaseModel, ABC):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
    )

    type: str

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    def basename(self) -> str:
        p = self.path
        return p.name if isinstance(p, Path) else p.split("/")[-1]

    def __str__(self) -> str:
        return self.basename

    @property
    @abstractmethod
    def extension(self) -> str:
        pass

    @property
    @abstractmethod
    def size(self) -> int:
        pass

    @property
    @abstractmethod
    def modify(self) -> datetime:
        pass


class BaseLocalFile(BaseFile, ABC):
    path: Path

    @property
    def name(self) -> str:
        return self.path.name

    async def get_hash(
        self, algorithm: str = "sha256", chunk_size: int = 1024 * 1024
    ) -> str:
        def _compute_hash():
            hash_obj = hashlib.new(algorithm)
            with open(self.path, "rb") as f:
                while chunk := f.read(chunk_size):
                    hash_obj.update(chunk)
            return hash_obj.hexdigest()

        return await anyio.to_thread.run_sync(_compute_hash)

    @abstractmethod
    async def load(self) -> Any:
        pass

    @abstractmethod
    async def stream(
        self,
        chunk_size: Optional[int] = None,
    ) -> AsyncGenerator[Any, None]:
        pass

    @property
    def extension(self) -> str:
        return self.path.suffix

    @property
    def size(self) -> int:
        return self.path.stat().st_size

    @property
    def modify(self) -> datetime:
        return datetime.fromtimestamp(self.path.stat().st_mtime)


class BaseTabularFile(BaseLocalFile, ABC):
    @property
    @abstractmethod
    def columns(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def rows(self) -> int:
        pass

    @abstractmethod
    async def load(self) -> pd.DataFrame:
        pass

    @abstractmethod
    async def stream(
        self,
        chunk_size: int = 10000,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        pass

    async def to_parquet(
        self,
        output_path: Optional[Union[str, Path]] = None,
        chunk_size: int = 10000,
    ) -> "BaseTabularFile":
        from pysus.api.extensions import ExtensionFactory

        if output_path is None:
            output_path = self.path.with_suffix(".parquet")

        output_path = Path(output_path).expanduser().resolve()
        writer = None

        pbar = tqdm(
            desc=f"Converting {self.basename}",
            unit=" rows",
            unit_scale=True,
        )

        try:
            async for chunk in self.stream(chunk_size=chunk_size):
                if chunk.empty:
                    continue

                rows_in_chunk = len(chunk)

                table = await anyio.to_thread.run_sync(
                    pa.Table.from_pandas,
                    chunk,
                )

                if writer is None:
                    writer = await anyio.to_thread.run_sync(
                        pq.ParquetWriter, output_path, table.schema
                    )

                await anyio.to_thread.run_sync(writer.write_table, table)

                pbar.update(rows_in_chunk)
                await anyio.sleep(0)
        finally:
            pbar.close()
            if writer:
                await anyio.to_thread.run_sync(writer.close)

        return await ExtensionFactory.instantiate(output_path)


class BaseCompressedFile(BaseLocalFile, ABC):
    @abstractmethod
    async def list_members(self) -> List[str]:
        pass

    @abstractmethod
    async def open_member(self, member_name: str) -> Any:
        pass

    @abstractmethod
    async def extract(
        self, target_dir: Optional[Path] = CACHEPATH
    ) -> List[BaseLocalFile]:
        pass

    async def stream(
        self,
        chunk_size: Optional[int] = None,
    ) -> AsyncGenerator[Any, None]:
        members = await self.list_members()
        for member in members:
            yield await self.open_member(member)
            await asyncio.sleep(0)


class SearchableMixin:
    def _matches(self, obj: Any, **kwargs) -> bool:
        for key, value in kwargs.items():
            obj_value = getattr(obj, key, None)
            if obj_value != value:
                return False
        return True


class BaseRemoteFile(BaseFile, SearchableMixin, ABC):
    parent: Union["BaseRemoteDataset", "BaseRemoteGroup"] = Field(exclude=True)
    _path: str = PrivateAttr()

    @property
    def path(self) -> str:
        return self._path

    @property
    def name(self) -> str:
        return self.basename

    @property
    def client(self) -> "BaseRemoteClient":
        if hasattr(self.parent, "client"):
            return self.parent.client
        return self.parent.dataset.client

    @property
    def year(self) -> Optional[int]:
        return None

    @property
    def month(self) -> Optional[int]:
        return None

    @property
    def state(self) -> Optional[State]:
        return None

    @abstractmethod
    async def _download(
        self,
        output: Optional[Path] = None,
        callback: Optional[Callable[[int], None]] = None,
    ) -> Path:
        pass

    async def download(
        self,
        output: Optional[Union[str, Path]] = None,
        callback: Optional[Callable[[int], None]] = None,
    ) -> BaseLocalFile:
        from pysus.api.extensions import ExtensionFactory

        if output is None:
            cache_dir = Path(CACHEPATH)
            cache_dir.mkdir(parents=True, exist_ok=True)
            dest = cache_dir / self.basename
        else:
            output_path = Path(output).expanduser().resolve()
            if output_path.is_dir():
                output_path.mkdir(parents=True, exist_ok=True)
                dest = output_path / self.basename
            else:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                dest = output_path

        local_path = await self._download(output=dest, callback=callback)

        return await ExtensionFactory.instantiate(local_path)


class BaseRemoteObject(BaseModel, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __str__(self) -> str:
        return self.name

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def long_name(self) -> str:
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        pass


class BaseRemoteGroup(BaseRemoteObject, SearchableMixin, ABC):
    dataset: "BaseRemoteDataset" = Field(exclude=True)
    _files: Optional[List["BaseRemoteFile"]] = PrivateAttr(default=None)

    @property
    def parent(self) -> "BaseRemoteDataset":
        return self.dataset

    @abstractmethod
    async def _fetch_files(self) -> List["BaseRemoteFile"]:
        pass

    @property
    async def files(self) -> List["BaseRemoteFile"]:
        if self._files is None:
            self._files = await self._fetch_files()
        return self._files

    async def search(self, **kwargs) -> List["BaseRemoteFile"]:
        all_files = await self.files
        if not kwargs:
            return all_files
        return [f for f in all_files if self._matches(f, **kwargs)]


class BaseRemoteDataset(BaseRemoteObject, SearchableMixin, ABC):
    client: "BaseRemoteClient" = Field(exclude=True)
    _content: Optional[
        List[
            Union[
                "BaseRemoteGroup",
                "BaseRemoteFile",
            ]
        ]
    ] = PrivateAttr(default=None)

    @abstractmethod
    async def _fetch_content(
        self,
    ) -> List[Union["BaseRemoteGroup", "BaseRemoteFile",]]:
        pass

    @property
    async def content(
        self,
    ) -> List[Union["BaseRemoteGroup", "BaseRemoteFile"]]:
        if self._content is None:
            self._content = await self._fetch_content()
        return self._content

    async def search(self, **kwargs) -> List["BaseRemoteFile"]:
        contents = await self.content

        matches = []
        for item in contents:
            if isinstance(item, BaseRemoteGroup):
                group_matches = await item.search(**kwargs)
                matches.extend(group_matches)
            elif isinstance(item, BaseRemoteFile):
                if self._matches(item, **kwargs):
                    matches.append(item)

        return matches


class BaseRemoteClient(BaseRemoteObject, ABC):
    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass

    @abstractmethod
    async def login(self, **kwargs) -> None:
        pass

    @abstractmethod
    async def datasets(self, **kwargs) -> List[BaseRemoteDataset]:
        pass

    @abstractmethod
    async def _download_file(
        self,
        file: BaseRemoteFile,
        output: Path,
        callback: Optional[Callable[[int], None]] = None,
    ) -> Path:
        pass
