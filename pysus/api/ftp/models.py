from __future__ import annotations

import os
from pathlib import Path
from abc import abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union

from pydantic import PrivateAttr
from pysus import CACHEPATH
from pysus.api.models import (
    BaseRemoteClient,
    BaseRemoteDataset,
    BaseRemoteFile,
    BaseRemoteGroup,
)

from .client import FTPFileInfo, FTPGroupInfo


class File(BaseRemoteFile):
    _info: FTPFileInfo = PrivateAttr()

    def __init__(self, **data):
        info = data.pop("_info", None)
        path = data.pop("path", None)

        super().__init__(**data)

        if info is not None:
            self._info = info

        if path is not None:
            self._path = path

    def __repr__(self) -> str:
        return self.name

    @property
    def extension(self) -> str:
        return Path(self.path).suffix

    @property
    def size(self) -> int:
        return self._info.get("size", 0)

    @property
    def modify(self) -> datetime:
        return self._info.get("modify")

    @property
    def group(self) -> Optional[str]:
        group_data = self._info.get("group")
        return group_data["name"] if group_data else None

    @property
    def group_info(self) -> Optional[FTPGroupInfo]:
        return self._info.get("group")

    @property
    def year(self) -> Optional[int]:
        return self._info.get("year")

    @property
    def month(self) -> Optional[int]:
        return self._info.get("month")

    @property
    def state(self) -> Optional[str]:
        return self._info.get("state")

    async def _download(
        self,
        output: Optional[Path] = None,
        callback: Optional[Callable[[int], None]] = None,
    ) -> Path:
        if output is None:
            cache_dir = Path(CACHEPATH)
            cache_dir.mkdir(parents=True, exist_ok=True)
            output = cache_dir / self.basename

        return await self.client._download_file(self, output, callback)


class Directory:
    def __init__(
        self,
        path: str,
        parent: Optional[Union[Directory, Dataset, Group]] = None,
        client: Optional[BaseRemoteClient] = None,
        formatter: Optional[Callable] = None,
        dataset: Optional[Dataset] = None,
    ):
        self.path = os.path.normpath(path)
        self.parent = parent
        self.dataset = dataset or (
            parent.dataset if hasattr(parent, "dataset") else None
        )
        self.client = client or (parent.client if parent else None)
        self.formatter = formatter or (parent.formatter if parent else None)
        self.name = os.path.basename(self.path) or "/"
        self.loaded = False
        self._content: List[Union[Directory, File]] = []

    @property
    async def content(self) -> List[Union[Directory, File]]:
        if not self.loaded:
            await self.load()
        return self._content

    async def load(self) -> None:
        raw_infos = await self.client._list_directory(self.path, self.formatter)
        self._content = []

        file_parent = (
            self
            if isinstance(self, (BaseRemoteGroup, BaseRemoteDataset))
            else self.dataset
        )

        for info in raw_infos:
            if info["type"] == "dir":
                self._content.append(
                    Directory(
                        path=f"{self.path}/{info['name']}",
                        parent=self,
                        dataset=self.dataset,
                    )
                )
            else:
                self._content.append(
                    File(
                        path=f"{self.path}/{info['name']}",
                        parent=file_parent,
                        type=info["type"],
                        _info=info,
                    )
                )
        self.loaded = True

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return f"<Directory: {self.path}>"


class Group(BaseRemoteGroup):
    path: str
    _long_name: str = PrivateAttr()
    _description: str = PrivateAttr()
    _dir: Directory = PrivateAttr()

    def __init__(
        self,
        path: str,
        dataset: BaseRemoteDataset,
        long_name: str,
        description: str = "",
    ):
        super().__init__(dataset=dataset, path=path)
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
        return os.path.basename(self.path)

    @property
    def long_name(self) -> str:
        return self._long_name

    @property
    def description(self) -> str:
        return self._description

    @property
    async def content(self) -> List[Union[Directory, File]]:
        return await self._dir.content

    async def _fetch_files(self) -> List[BaseRemoteFile]:
        items = await self.content
        return [item for item in items if isinstance(item, BaseRemoteFile)]


class Dataset(BaseRemoteDataset):
    paths: List[Directory] = []
    group_definitions: Dict[str, str] = {}
    _content: Optional[
        List[
            Union[
                Group,
                Directory,
                File,
            ]
        ]
    ] = PrivateAttr(default=None)

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
    def formatter(self, filename: str) -> Dict[str, Any]:
        pass

    async def _fetch_content(self) -> List[Union[Group, Directory, File]]:
        results = []

        for root_dir in self.paths:
            root_dir.client = self.client
            root_dir.formatter = self.formatter
            root_dir.dataset = self

            items = await root_dir.content

            for item in items:
                if isinstance(item, Directory):
                    if item.name in self.group_definitions:
                        group = Group(
                            path=item.path,
                            dataset=self,
                            long_name=self.group_definitions[item.name],
                        )
                        results.append(group)
                    else:
                        results.append(item)

                elif isinstance(item, File):
                    results.append(item)

        return results

    def __repr__(self) -> str:
        return f"<Database: {self.name}>"
