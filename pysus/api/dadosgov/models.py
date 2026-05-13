"""Internal domain models for datasets, groups, and files from dados.gov.br."""

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
    """A downloadable file from a dados.gov.br dataset."""

    record: Recurso
    type: str = "File"
    _metadata: dict[str, Any] = PrivateAttr(default_factory=dict)

    def __init__(self, **data):
        """Initialize the File with optional metadata."""
        metadata = data.pop("_metadata", {})
        super().__init__(**data)
        self._metadata = metadata
        self._path = self.record.url

    def __repr__(self):
        return self.basename

    def model_post_init(self, __context: Any) -> None:
        """Fetch remote metadata if size or modify date is missing."""
        if not self.record.api_size or not self.record.last_modified:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self.fetch_metadata())
            except RuntimeError:
                pass

        return

    @property
    def extension(self) -> str:
        """Return the file extension."""
        if self.record.file_name:
            return pathlib.Path(self.record.file_name).suffix
        return pathlib.Path(self.record.url.split("/")[-1].split("?")[0]).suffix

    @property
    def size(self) -> int:
        """Return the file size in bytes."""
        return self.record.api_size or 0

    @property
    def modify(self) -> datetime:
        """Return the last modification date."""
        m = self.record.last_modified
        if not m:
            raise ValueError("File requires a modify date")
        return m

    @property
    def year(self) -> int | None:
        """Return the inferred year from metadata."""
        return self._metadata.get("year")

    @property
    def month(self) -> int | None:
        """Return the inferred month from metadata."""
        return self._metadata.get("month")

    @property
    def state(self) -> State | None:
        """Return the inferred state from metadata."""
        return self._metadata.get("state")

    async def fetch_metadata(self) -> None:
        """Fetch file size and last-modified from the remote server."""
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
        """Download the file to a local path."""
        if not output:
            output = CACHEPATH / self.name
        return await self.client._download_file(self, output, callback=callback)

    async def fetch_size(self) -> int:
        """Fetch the remote file size and update the local record."""
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
    """A group of files within a dataset."""

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
        """Initialize the Group with a dataset record and optional formatter."""
        super().__init__(dataset=dataset)
        self.record = record
        self._formatter = formatter

    def __repr__(self):
        return self.name

    @property
    def name(self) -> str:
        """Return the group slug name."""
        return self.record.slug

    @property
    def long_name(self) -> str:
        """Return the group title."""
        return self.record.title

    @property
    def description(self) -> str:
        """Return an empty description."""
        return ""

    async def _fetch_files(self) -> list[BaseRemoteFile]:
        """Build File objects from the underlying resources."""
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
    """A health dataset available through dados.gov.br."""

    ids: list[str] = []
    client: "DadosGov"

    def __repr__(self):
        return self.name

    @abstractmethod
    def formatter(self, filename: str) -> dict[str, Any]:
        """Extract structured metadata from a filename."""
        pass

    async def _fetch_content(self) -> list[Group]:
        """Fetch all groups belonging to this dataset."""
        items: list[Group] = []
        client: "DadosGov" = self.client
        if self.ids:
            for group_id in self.ids:
                record = await client.get_dataset(group_id)
                items.append(
                    Group(record=record, dataset=self, formatter=self.formatter)
                )
        return items
