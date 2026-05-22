"""Application-level models for DuckLake remote resources.

Wraps catalog ORM records into BaseRemoteFile, BaseRemoteDataset,
and BaseRemoteGroup interfaces used by the rest of PySUS.
"""

import hashlib
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, Union

import anyio
from pydantic import Field
from pysus import CACHEPATH
from pysus.api.models import (
    BaseRemoteClient,
    BaseRemoteDataset,
    BaseRemoteFile,
    BaseRemoteGroup,
)

from .catalog import CatalogDataset, CatalogFile, DatasetGroup


class File(BaseRemoteFile):
    """A remote file in the DuckLake catalog with download and verification.

    Parameters
    ----------
    record : CatalogFile
        The underlying ORM record.
    type : str, optional
        File type identifier (default ``"remote"``).
    dataset : Any
        The parent dataset object.
    group : Any, optional
        The parent group object, if any.
    """

    record: CatalogFile = Field(exclude=True)
    type: str = "remote"
    dataset: Any
    group: Any = None

    @property
    def basename(self) -> str:
        """Return the file name without directory components.

        Returns
        -------
        str
            The base file name.
        """
        return self.path.name

    @property
    def extension(self) -> str:
        """Return the file extension including the leading dot.

        Returns
        -------
        str
            File extension (e.g. ``'.csv'``).
        """
        return self.path.suffix

    @property
    def size(self) -> int:
        """Return the file size in bytes.

        Returns
        -------
        int
            File size in bytes.
        """
        return self.record.size

    @property
    def modify(self) -> datetime:
        """Return the last-modified timestamp.

        Returns
        -------
        datetime
            The last modification timestamp.
        """
        return self.record.modified

    @property
    def rows(self) -> int:
        """Return the number of rows in the file.

        Returns
        -------
        int
            Row count.
        """
        return self.record.rows

    @property
    def sha256(self) -> str | None:
        """Return the SHA-256 hash of the file, if available.

        Returns
        -------
        str or None
            SHA-256 hex digest, or None if not recorded.
        """
        return self.record.sha256

    async def _download(
        self,
        output: Path | None = None,
        callback: Callable[[int, int], None] | None = None,
    ) -> Path:
        """Download the file from object storage to the given output path."""
        if not output:
            output = CACHEPATH / self.name

        return await self.client._download_file(
            self,
            output,
            callback=callback,
        )

    async def verify(self, path: Path) -> bool:
        """Verify the file matches the recorded SHA-256 hash.

        Parameters
        ----------
        path : Path
            Path to the downloaded file on disk.

        Returns
        -------
        bool
            True if the hash matches or no hash is recorded, False otherwise.
        """
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


class DuckDataset(BaseRemoteDataset):
    """A dataset from the DuckLake catalog, containing groups and files.

    Parameters
    ----------
    record : CatalogDataset
        The underlying ORM record.
    client : BaseRemoteClient
        The parent client instance.
    """

    record: CatalogDataset = Field(exclude=True)
    client: BaseRemoteClient = Field(exclude=True)

    def __repr__(self) -> str:
        """Return a string representation of the dataset.

        Returns
        -------
        str
            The uppercased dataset name.
        """
        return self.name.upper()

    @property
    def name(self) -> str:
        """Return the short name of the dataset.

        Returns
        -------
        str
            The dataset short name.
        """
        return self.record.name

    @property
    def long_name(self) -> str:
        """Return the human-readable name of the dataset.

        Returns
        -------
        str
            The dataset display name, falling back to the short name.
        """
        return (
            self.record.dataset_metadata.long_name
            if self.record.dataset_metadata
            else self.name
        )

    @property
    def description(self) -> str:
        """Return the description of the dataset.

        Returns
        -------
        str
            The dataset description, or an empty string if unavailable.
        """
        return (
            self.record.dataset_metadata.description
            if self.record.dataset_metadata
            else ""
        )

    async def _fetch_content(self) -> list[Union["DuckGroup", File]]:
        """Fetch groups and files belonging to this dataset."""
        items: list[Union["DuckGroup", File]] = []

        if self.record.groups:
            items.extend(
                [DuckGroup(record=g, dataset=self) for g in self.record.groups]
            )

        if self.record.files:
            items.extend(
                [
                    File(
                        path=f.path,
                        record=f,
                        dataset=self,
                    )
                    for f in self.record.files
                ]
            )

        return items


class DuckGroup(BaseRemoteGroup):
    """A group of related files within a DuckLake dataset.

    Parameters
    ----------
    record : DatasetGroup
        The underlying ORM record.
    dataset : DuckDataset
        The parent dataset instance.
    """

    record: DatasetGroup = Field(exclude=True)
    dataset: DuckDataset = Field(exclude=True)

    @property
    def name(self) -> str:
        """Return the short name of the group.

        Returns
        -------
        str
            The group short name.
        """
        return self.record.name

    @property
    def long_name(self) -> str:
        """Return the human-readable name of the group.

        Returns
        -------
        str
            The group display name, falling back to the short name.
        """
        return (
            self.record.group_metadata.long_name
            if self.record.group_metadata
            else self.name
        )

    @property
    def description(self) -> str:
        """Return the description of the group.

        Returns
        -------
        str
            The group description, or an empty string if unavailable.
        """
        if self.record.group_metadata:
            return self.record.group_metadata.description
        return ""

    async def _fetch_files(self) -> list[BaseRemoteFile]:
        """Fetch the list of files belonging to this group."""
        files: list[BaseRemoteFile] = [
            File(
                path=f.path,
                record=f,
                group=self,
                dataset=self.dataset,
            )
            for f in self.record.files
        ]
        return files
