import hashlib
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional, Union

import anyio
from pydantic import Field
from pysus.api.models import (
    BaseRemoteClient,
    BaseRemoteDataset,
    BaseRemoteFile,
    BaseRemoteGroup,
)

from .catalog import CatalogDataset, CatalogFile, DatasetGroup


class File(BaseRemoteFile):
    record: CatalogFile = Field(exclude=True)
    parent: Union["Dataset", "Group"] = Field(exclude=True)

    type: str = "remote"

    @property
    def path(self) -> Path:
        return Path(self.record.path)

    @property
    def basename(self) -> str:
        return self.path.name

    @property
    def extension(self) -> str:
        return self.path.suffix

    @property
    def size(self) -> int:
        return self.record.size

    @property
    def modify(self) -> datetime:
        return self.record.modified

    @property
    def rows(self) -> int:
        return self.record.rows

    @property
    def sha256(self) -> Optional[str]:
        return self.record.sha256

    async def _download(
        self, output: Path, callback: Optional[Callable[[int], None]] = None
    ) -> Path:
        return await self.client._download_file(
            self, output, callback=callback
        )

    async def verify(self, path: Path) -> bool:
        if not self.sha256:
            return True

        def _calculate():
            sha256_hash = hashlib.sha256()
            with open(path, "rb") as f:
                for byte_block in iter(lambda: f.read(8192), b""):
                    sha256_hash.update(byte_block)
            return sha256_hash.hexdigest()

        actual_hash = await anyio.to_thread.run_sync(_calculate)
        return actual_hash == self.sha256


class Group(BaseRemoteGroup):
    record: DatasetGroup = Field(exclude=True)
    dataset: "Dataset" = Field(exclude=True)

    @property
    def name(self) -> str:
        return self.record.name

    @property
    def long_name(self) -> str:
        return (
            self.record.group_metadata.long_name
            if self.record.group_metadata
            else self.name
        )

    @property
    def description(self) -> str:
        if self.record.group_metadata:
            return self.record.group_metadata.description
        return ""

    async def files(self, **kwargs) -> List[File]:
        return [File(record=f, parent=self) for f in self.record.files]


class Dataset(BaseRemoteDataset):
    record: CatalogDataset = Field(exclude=True)
    client: BaseRemoteClient = Field(exclude=True)

    @property
    def name(self) -> str:
        return self.record.name

    @property
    def long_name(self) -> str:
        return (
            self.record.dataset_metadata.long_name
            if self.record.dataset_metadata
            else self.name
        )

    @property
    def description(self) -> str:
        return (
            self.record.dataset_metadata.description
            if self.record.dataset_metadata
            else ""
        )

    async def content(self, **kwargs) -> List[Union[Group, File]]:
        items = []

        if self.record.groups:
            items.extend(
                [Group(record=g, dataset=self) for g in self.record.groups],
            )

        if self.record.files:
            items.extend(
                [File(record=f, parent=self) for f in self.record.files],
            )

        return items
