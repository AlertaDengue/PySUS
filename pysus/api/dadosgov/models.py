import asyncio
import pathlib
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime as dt
from typing import Any

import httpx
from pydantic import PrivateAttr
from pysus.api.models import (
    BaseRemoteClient,
    BaseRemoteDataset,
    BaseRemoteFile,
    BaseRemoteGroup,
)

from .client import ConjuntoDados, Recurso


class File(BaseRemoteFile):
    record: Recurso
    type: str | None = "remote"
    _metadata: dict[str, Any] = PrivateAttr(default_factory=dict)

    def __init__(self, **data):
        metadata = data.pop("_metadata", {})
        super().__init__(**data)
        self._metadata = metadata

    def __repr__(self):
        return self.basename

    def model_post_init(self, __context: Any) -> None:
        if self.record.api_size is None or self.record.api_size == 0:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self.fetch_size())
            except RuntimeError:
                pass

    @property
    def path(self) -> str:
        return self.record.url

    @property
    def extension(self) -> str:
        if self.record.file_name:
            return pathlib.Path(self.record.file_name).suffix
        return pathlib.Path(
            self.record.url.split("/")[-1].split("?")[0]
        ).suffix

    @property
    def size(self) -> int:
        return self.record.api_size or 0

    @property
    def modify(self) -> dt:
        return self.record.last_modified

    @property
    def year(self) -> int | None:
        return self._metadata.get("year")

    @property
    def month(self) -> int | None:
        return self._metadata.get("month")

    @property
    def state(self) -> str | None:
        return self._metadata.get("state")

    async def _download(
        self,
        output: pathlib.Path | None = None,
        callback: Callable[[int], None] | None = None,
    ) -> pathlib.Path:
        return await self.client._download_file(
            self, output, callback=callback
        )

    async def fetch_size(self) -> int:
        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=3,
            ) as client:
                response = await client.head(self.path)

                if response.status_code == 405:
                    response = await client.get(
                        self.path, headers={"Range": "bytes=0-0"}
                    )

                remote_size = int(response.headers.get("Content-Length", 0))

                if remote_size > 0:
                    self.record.api_size = remote_size

                return remote_size
        except Exception:
            return 0


class Group(BaseRemoteGroup):
    record: ConjuntoDados
    _formatter: Callable[[Recurso, "Group"], dict[str, Any]] | None = (
        PrivateAttr(default=None)
    )

    def __init__(
        self,
        record: ConjuntoDados,
        dataset: BaseRemoteDataset,
        formatter: Callable | None = None,
    ):
        super().__init__(dataset=dataset)
        self.record = record
        self._formatter = formatter

    def __repr__(self):
        return self.name

    @property
    def name(self) -> str:
        return self.record.slug

    @property
    def long_name(self) -> str:
        return self.record.title

    @property
    def description(self) -> str:
        return ""

    async def _fetch_files(self) -> list[File]:
        files = []
        for recurso in self.record.resources:
            metadata = (
                self._formatter(recurso, self) if self._formatter else {}
            )
            file = File(record=recurso, parent=self, _metadata=metadata)
            files.append(file)
        return files


class Dataset(BaseRemoteDataset, ABC):
    ids: list[str]

    def __repr__(self):
        return self.name

    @property
    @abstractmethod
    def formatter(self) -> Callable[[Recurso, Group], dict[str, Any]]:
        pass

    async def _fetch_content(self) -> list[Group]:
        items: list[Group] = []
        client: BaseRemoteClient = self.client
        if self.ids:
            for group_id in self.ids:
                record = await client.get_dataset(group_id)
                items.append(
                    Group(
                        record=record, dataset=self, formatter=self.formatter
                    )
                )
        return items
