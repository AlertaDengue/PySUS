import asyncio
import pathlib
from abc import abstractmethod
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any

import httpx
from dateparser import parse  # type: ignore[import-untyped]
from pydantic import PrivateAttr
from pysus import CACHEPATH
from pysus.api.models import BaseRemoteDataset, BaseRemoteFile, BaseRemoteGroup
from pysus.api.types import State

from .client import ConjuntoDados, Recurso

if TYPE_CHECKING:
    from .client import DadosGov


class File(BaseRemoteFile):
    record: Recurso
    type: str = "File"
    _metadata: dict[str, Any] = PrivateAttr(default_factory=dict)

    def __init__(self, **data):
        metadata = data.pop("_metadata", {})
        super().__init__(**data)
        self._metadata = metadata
        self._path = self.record.url

    def __repr__(self):
        return self.basename

    def model_post_init(self, __context: Any) -> None:
        if not self.record.api_size or not self.record.last_modified:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self.fetch_metadata())
            except RuntimeError:
                pass

        return

    @property
    def extension(self) -> str:
        if self.record.file_name:
            return pathlib.Path(self.record.file_name).suffix
        return pathlib.Path(self.record.url.split("/")[-1].split("?")[0]).suffix

    @property
    def size(self) -> int:
        return self.record.api_size or 0

    @property
    def modify(self) -> datetime:
        m = self.record.last_modified
        if not m:
            raise ValueError("File requires a modify date")
        return m

    @property
    def year(self) -> int | None:
        return self._metadata.get("year")

    @property
    def month(self) -> int | None:
        return self._metadata.get("month")

    @property
    def state(self) -> State | None:
        return self._metadata.get("state")

    async def fetch_metadata(self) -> None:
        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=5,
            ) as client:
                response = await client.head(str(self.path))

                if response.status_code == 405:
                    response = await client.get(
                        str(self.path), headers={"Range": "bytes=0-0"}
                    )

                size_str = response.headers.get("Content-Length")
                if size_str:
                    self.record.api_size = int(size_str)

                last_mod_str = response.headers.get("Last-Modified")
                if last_mod_str:
                    try:
                        self.record.last_modified = parse(last_mod_str)
                    except (TypeError, ValueError):
                        pass
        except Exception:  # noqa: B902
            pass

    async def _download(
        self,
        output: pathlib.Path | None = None,
        callback: Callable[[int], None] | None = None,
    ) -> pathlib.Path:
        if not output:
            output = CACHEPATH / self.name
        return await self.client._download_file(self, output, callback=callback)

    async def fetch_size(self) -> int:
        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=3,
            ) as client:
                response = await client.head(str(self.path))

                if response.status_code == 405:
                    response = await client.get(
                        str(self.path), headers={"Range": "bytes=0-0"}
                    )

                remote_size = int(response.headers.get("Content-Length", 0))

                if remote_size > 0:
                    self.record.api_size = remote_size

                return remote_size
        except Exception:  # noqa: B902
            return 0


class Group(BaseRemoteGroup):
    record: ConjuntoDados
    _formatter: (
        Callable[
            [Recurso, "Group"],
            dict[str, Any],
        ]
        | None
    ) = PrivateAttr(default=None)

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

    async def _fetch_files(self) -> list[BaseRemoteFile]:
        files: list[BaseRemoteFile] = []
        for recurso in self.record.resources:
            metadata = self._formatter(recurso, self) if self._formatter else {}
            file = File(
                record=recurso,
                dataset=self.dataset,
                group=self,
                _metadata=metadata,
            )
            files.append(file)
        return files


class Dataset(BaseRemoteDataset):
    ids: list[str] = []
    client: DadosGov

    def __repr__(self):
        return self.name

    @abstractmethod
    def formatter(self, filename: str) -> dict[str, Any]:
        pass

    async def _fetch_content(self) -> list[Group]:
        items: list[Group] = []
        client: DadosGov = self.client
        if self.ids:
            for group_id in self.ids:
                record = await client.get_dataset(group_id)
                items.append(
                    Group(record=record, dataset=self, formatter=self.formatter)
                )
        return items
