from typing import Optional, Union, List, Any, Generator, AsyncGenerator
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
import dateparser

from pydantic import BaseModel, ConfigDict, field_validator
import pyarrow as pa
import pyarrow.parquet as pq
import anyio
import pandas as pd
from tqdm.asyncio import tqdm

from pysus.data.local import ParquetSet
from pysus import CACHEPATH


class FileDescription(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)
    name: str
    group: str
    year: int
    size: int
    last_update: datetime
    uf: Optional[str] = None
    month: Optional[str] = None
    disease: Optional[str] = None

    @field_validator("last_update", mode="before")
    @classmethod
    def parse_modify_date(cls, v: Any) -> datetime:
        if isinstance(v, datetime):
            return v
        parsed = dateparser.parse(str(v))
        return parsed if parsed else datetime.now()


class BaseFormatter(BaseModel, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @abstractmethod
    def parse(self, filename: str) -> dict:
        pass


class BaseTabularFile(ABC):
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


class BaseFile(BaseModel, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    basename: str
    path: Path
    extension: str

    def __str__(self) -> str:
        return self.basename

    def __repr__(self):
        return self.basename


class BaseLocalFile(BaseFile, ABC):
    path: Path

    @abstractmethod
    def load(self) -> Any:
        pass

    @abstractmethod
    def stream(
        self,
        chunk_size: Optional[int] = None,
    ) -> Generator[Any, None, None]:
        pass


class BaseRemoteFile(BaseFile, ABC):
    path: str

    @abstractmethod
    def describe(
        self, formatter: Optional["BaseFormatter"] = None
    ) -> "FileDescription":
        pass

    @abstractmethod
    async def _download(self, destination: Path) -> BaseLocalFile:
        pass

    async def download(self, output: Union[str, Path]) -> BaseLocalFile:
        output_path = Path(output).expanduser().resolve()

        if output_path.is_dir():
            dest = output_path / self.basename
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            dest = output_path

        return await self._download(destination=dest)


class BaseCompressedFile(BaseLocalFile, ABC):
    @abstractmethod
    def list_members(self) -> List[str]:
        pass

    @abstractmethod
    def open_member(self, member_name: str) -> Any:
        pass

    @abstractmethod
    def extract(self, target_dir: Optional[Path] = CACHEPATH) -> List[Path]:
        pass

    def stream(
        self,
        chunk_size: Optional[int] = None,
    ) -> Generator[Any, None, None]:
        pass


class BaseRemoteDataset(BaseModel, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    formatter: Optional[BaseFormatter] = None

    @abstractmethod
    async def get_files(self, **kwargs) -> List[BaseRemoteFile]:
        pass

    async def download(
        self,
        files: Union[List[BaseRemoteFile], BaseRemoteFile],
        output: Union[str, Path] = CACHEPATH,
    ) -> List[ParquetSet]:
        output = Path(output).expanduser().resolve()
        file_list = [files] if not isinstance(files, list) else files

        tasks = []
        for i, f in enumerate(file_list):
            if not output.is_dir() and len(file_list) > 1:
                name = output.parent / f"{output.stem}_{i}{output.suffix}"
                tasks.append(f.download(output=name))
            else:
                tasks.append(f.download(output=output))

        res = await tqdm.gather(*tasks, desc="Downloading")
        return [res] if not isinstance(res, list) else res


class BaseRemoteClient(BaseModel, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @abstractmethod
    async def connect(self):
        pass

    @abstractmethod
    async def close(self):
        pass
