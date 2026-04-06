from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncGenerator, List, Optional, Union
import asyncio
import hashlib

from pydantic import BaseModel, ConfigDict, Field
from tqdm.asyncio import tqdm
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import anyio

from pysus import CACHEPATH


class BaseFile(BaseModel, ABC):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
    )

    type: str

    def __str__(self) -> str:
        return self.path.name

    def __repr__(self):
        return self.path.name

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


class BaseRemoteFile(BaseFile, ABC):
    parent: Union["BaseRemoteDataset", "BaseRemoteGroup"] = Field(exclude=True)

    @property
    def client(self) -> "BaseRemoteClient":
        if hasattr(self.parent, "client"):
            return self.parent.client
        return self.parent.dataset.client

    @abstractmethod
    async def _download(self, output: Path) -> Path:
        pass

    async def download(self, output: Union[str, Path]) -> BaseLocalFile:
        from pysus.api.extensions import ExtensionFactory

        output_path = Path(output).expanduser().resolve()

        if output_path.is_dir():
            output_path.mkdir(parents=True, exist_ok=True)
            dest = output_path / self.basename
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            dest = output_path

        local_path = await self._download(output=dest)

        return await ExtensionFactory.instantiate(local_path)


class BaseRemoteGroup(BaseModel, ABC):
    dataset: "BaseRemoteDataset" = Field(exclude=True)

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

    @abstractmethod
    async def files(self, **kwargs) -> List["BaseRemoteFile"]:
        pass

    def __str__(self):
        return self.name


class BaseRemoteDataset(BaseModel, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    client: "BaseRemoteClient" = Field(exclude=True)

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

    @abstractmethod
    async def groups(self) -> List["BaseRemoteGroup"]:
        pass

    @abstractmethod
    async def files(self, **kwargs) -> List["BaseRemoteFile"]:
        pass

    async def children(
        self,
    ) -> Union[
        List["BaseRemoteGroup"],
        List["BaseRemoteFile"],
    ]:
        groups = await self.groups()
        if groups:
            return groups
        return await self.files()


class BaseRemoteClient(BaseModel, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)

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
    async def _download_file(self, file: BaseRemoteFile, output: Path) -> Path:
        pass
