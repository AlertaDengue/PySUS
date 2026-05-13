"""Data model classes for FTP directories, files, groups and datasets."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import PrivateAttr
from pysus import CACHEPATH
from pysus.api.models import (
    BaseRemoteClient,
    BaseRemoteDataset,
    BaseRemoteFile,
    BaseRemoteGroup,
)
from pysus.api.types import State

from .client import FTP, FTPFileInfo


class File(BaseRemoteFile):
    """A single file on the DATASUS FTP server with parsed metadata."""

    _info: FTPFileInfo = PrivateAttr()

    def __init__(self, **data):
        """Initialise the File with raw FTP metadata."""
        info = data.pop("_info", None)
        if "path" not in data and info and "path" in info:
            data["path"] = info["path"]

        super().__init__(**data)

        self._info = info
        group_data = self._info.get("group")
        if group_data:
            self.group = Group(
                path=str(self.path.parent),
                name=group_data.get("name", ""),
                dataset=self.dataset,
                long_name=group_data.get("long_name", ""),
                description=group_data.get("description", ""),
            )

    def __repr__(self) -> str:
        """Return the file name as its string representation."""
        return self.name

    @property
    def extension(self) -> str:
        """Return the file extension (e.g. .dbc, .dbf)."""
        return Path(self.path).suffix

    @property
    def size(self) -> int:
        """Return the file size in bytes."""
        return self._info.get("size", 0)

    @property
    def modify(self) -> datetime:
        """Return the last modification timestamp."""
        m = self._info.get("modify")
        if not m:
            raise ValueError("File requires a modify date")
        return m

    @property
    def year(self) -> int | None:
        """Return the data year extracted from the filename, if available."""
        return self._info.get("year")

    @property
    def month(self) -> int | None:
        """Return the data month extracted from the filename, if available."""
        return self._info.get("month")

    @property
    def state(self) -> State | None:
        """Return the state code extracted from the filename, if available."""
        return self._info.get("state", None)

    async def _download(
        self,
        output: Path | None = None,
        callback: Callable[[int], None] | None = None,
    ) -> Path:
        """Download this file to a local path, optionally reporting progress."""
        if output is None:
            cache_dir = Path(CACHEPATH)
            cache_dir.mkdir(parents=True, exist_ok=True)
            output = cache_dir / self.basename

        return await self.client._download_file(self, output, callback)


class Directory:
    """A remote FTP directory lazily loaded into files and subdirectories."""

    def __init__(
        self,
        path: str,
        parent: Directory | Dataset | Group | None = None,
        client: BaseRemoteClient | None = None,
        formatter: Callable | None = None,
        dataset: Dataset | None = None,
    ):
        """Initialise the Directory with a remote path and optional context."""
        self.path = os.path.normpath(path)
        self.parent = parent
        self.dataset = dataset or getattr(parent, "dataset", None)
        self.client = client or getattr(parent, "client", None)
        self.formatter = formatter or getattr(parent, "formatter", None)
        self.name = os.path.basename(self.path) or "/"
        self.loaded = False
        self._content: list[Directory | File] = []

    @property
    async def content(self) -> list[Directory | File]:
        """Return the directory contents, loading from FTP if not yet cached."""
        if not self.loaded:
            await self.load()
        return self._content

    async def load(self) -> None:
        """Fetch and parse the directory listing from the FTP server."""
        if not isinstance(self.client, FTP):
            raise ValueError("no ftp client found")
        raw_infos = await self.client._list_directory(
            self.path,
            self.formatter,
        )
        self._content = []

        current_group = (
            self.parent if isinstance(self.parent, BaseRemoteGroup) else None
        )

        for info in raw_infos:
            item_path = f"{self.path}/{info['name']}"
            if info["type"] == "dir":
                self._content.append(
                    Directory(
                        path=item_path,
                        parent=self,
                        dataset=self.dataset,
                    )
                )
            else:
                self._content.append(
                    File(
                        path=item_path,
                        dataset=self.dataset,
                        group=current_group,
                        type=info["type"],
                        _info=info,
                    )
                )
        self.loaded = True

    def __str__(self) -> str:
        """Return the normalised directory path."""
        return self.path

    def __repr__(self) -> str:
        """Return a debug representation of this directory."""
        return f"<Directory: {self.path}>"


class Group(BaseRemoteGroup):
    """A group of related files within a dataset (e.g. all files of a type)."""

    path: str
    _name: str = PrivateAttr()
    _long_name: str = PrivateAttr()
    _description: str = PrivateAttr()
    _dir: Directory = PrivateAttr()

    def __init__(
        self,
        name: str,
        path: str,
        dataset: Dataset,
        long_name: str,
        description: str = "",
        **data: Any,
    ):
        """Initialise the Group with metadata and a directory reference."""
        data.update({"dataset": dataset, "path": path})
        super().__init__(**data)

        self._name = name
        self._long_name = long_name
        self._description = description
        self._dir = Directory(
            path=path,
            client=dataset.client,
            formatter=dataset.formatter,
            dataset=dataset,
            parent=self,
        )

    @property
    def name(self) -> str:
        """Return the group short code (e.g. 'RD', 'PA')."""
        return self._name

    @property
    def long_name(self) -> str:
        """Return the human-readable group name."""
        return self._long_name

    @property
    def description(self) -> str:
        """Return the group description."""
        return self._description

    @property
    async def content(self) -> list[Directory | File]:
        """Return the contents of the underlying directory."""
        return await self._dir.content

    async def _fetch_files(self) -> list[BaseRemoteFile]:
        """Return only the file entries from this group's directory."""
        items = await self.content
        return [item for item in items if isinstance(item, BaseRemoteFile)]


class Dataset(BaseRemoteDataset, ABC):
    """Abstract base for a DATASUS dataset, providing file discovery via FTP."""

    paths: list[Directory] = []
    group_definitions: dict[str, str] = {}

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the dataset short name."""

    @property
    @abstractmethod
    def long_name(self) -> str:
        """Return the dataset full name in Portuguese."""

    @property
    @abstractmethod
    def description(self) -> str:
        """Return a description of the dataset's purpose."""

    @abstractmethod
    def formatter(self, filename: str) -> dict[str, Any]:
        """Parse a filename into metadata (group, state, year, etc.)."""

    async def _fetch_content(
        self,
    ) -> Sequence[BaseRemoteGroup | BaseRemoteFile]:
        """Walk the dataset's root directories and return groups and files."""
        results: list[BaseRemoteGroup | BaseRemoteFile] = []

        for root_dir in self.paths:
            root_dir.client = self.client
            root_dir.formatter = self.formatter
            root_dir.dataset = self

            if not isinstance(root_dir, Directory):
                raise RuntimeError(f"Directory {root_dir} not instantiated")

            items = await root_dir.content

            for item in items:
                if isinstance(item, Directory):
                    if item.name in self.group_definitions:
                        group = Group(
                            path=item.path,
                            name=item.name,
                            dataset=self,
                            long_name=self.group_definitions[item.name],
                        )
                        results.append(group)
                    else:
                        results.append(item)  # type: ignore

                elif isinstance(item, File):
                    results.append(item)

        return results

    def __repr__(self) -> str:
        """Return the dataset short name as its string representation."""
        return self.name
