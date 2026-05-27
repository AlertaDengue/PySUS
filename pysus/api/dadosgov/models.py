"""Internal domain models for datasets, groups, and files from dados.gov.br."""

import asyncio
import pathlib
import re
from abc import abstractmethod
from collections.abc import Callable
from datetime import datetime
from typing import Any

import httpx
from dateparser import parse  # type: ignore[import-untyped]
from pydantic import PrivateAttr
from pysus import CACHEPATH
from pysus.api.models import BaseRemoteDataset, BaseRemoteFile, BaseRemoteGroup
from pysus.api.types import State

from .client import ConjuntoDados, DadosGov, Recurso

_FORMAT_RE = re.compile(r"[._](csv|json|xml)(\.zip)?$", re.IGNORECASE)


def _dedup_entries(
    entries: list[tuple[str, Any, dict]],
) -> list[tuple[str, Any, dict]]:
    """If the same file exists in CSV, JSON and XML, keep only CSV."""
    grouped: dict[str, list[tuple[str, str, Any, dict]]] = {}
    for filename, recurso, metadata in entries:
        m = _FORMAT_RE.search(filename)
        if m:
            stem = filename[: m.start()]
            fmt = m.group(1).lower()
            grouped.setdefault(stem, []).append((fmt, filename, recurso, metadata))
        else:
            grouped.setdefault(filename, []).append(("", filename, recurso, metadata))

    result: list[tuple[str, Any, dict]] = []
    for _, items in grouped.items():
        formats = {fmt for fmt, _, _, _ in items}
        if "csv" in formats:
            for fmt, filename, recurso, metadata in items:
                if fmt == "csv":
                    result.append((filename, recurso, metadata))
        else:
            for _, filename, recurso, metadata in items:
                result.append((filename, recurso, metadata))
    return result


class File(BaseRemoteFile):
    """A downloadable file from a dados.gov.br dataset."""

    record: Recurso
    type: str = "File"
    _metadata: dict[str, Any] = PrivateAttr(default_factory=dict)

    def __init__(self, **data):
        """Initialize the File with optional metadata.

        Parameters
        ----------
        **data
            Keyword arguments including an optional ``_metadata`` dict
            that is stored on the private attribute ``_metadata``.
        """
        metadata = data.pop("_metadata", {})
        super().__init__(**data)
        self._metadata = metadata

    def __repr__(self):
        """Return the file basename as its string representation."""
        return self.basename

    def model_post_init(self, __context: Any) -> None:
        """Fetch remote metadata if size or modify date is missing.

        If both ``api_size`` and ``last_modified`` are falsy, schedules a
        background task to fetch metadata from the remote server.

        Parameters
        ----------
        __context : Any
            Pydantic validation context (unused).
        """
        if not self.record.api_size or not self.record.last_modified:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self.fetch_metadata())
            except RuntimeError:
                pass

        return

    @property
    def extension(self) -> str:
        """Return the file extension.

        Returns
        -------
        str
            The file extension (e.g., ``".csv"``, ``".zip"``).
        """
        if self.record.file_name:
            return pathlib.Path(self.record.file_name).suffix
        return pathlib.Path(self.record.url.split("/")[-1].split("?")[0]).suffix

    @property
    def size(self) -> int:
        """Return the file size in bytes.

        Returns
        -------
        int
            The file size, or 0 if unknown.
        """
        return self.record.api_size or 0

    @property
    def modify(self) -> datetime:
        """Return the last modification date.

        Returns
        -------
        datetime
            The last modification datetime.

        Raises
        ------
        ValueError
            If the modification date has not been set.
        """
        m = self.record.last_modified
        if not m:
            raise ValueError("File requires a modify date")
        return m

    @property
    def year(self) -> int | None:
        """Return the inferred year from metadata.

        Returns
        -------
        int or None
            The year if present in metadata, otherwise None.
        """
        return self._metadata.get("year")

    @property
    def month(self) -> int | None:
        """Return the inferred month from metadata.

        Returns
        -------
        int or None
            The month if present in metadata, otherwise None.
        """
        return self._metadata.get("month")

    @property
    def state(self) -> State | None:
        """Return the inferred state from metadata.

        Returns
        -------
        State or None
            The state abbreviation if present in metadata, otherwise None.
        """
        return self._metadata.get("state")

    async def fetch_metadata(self) -> None:
        """Fetch file size and last-modified from the remote server.

        Updates ``record.api_size`` and ``record.last_modified`` in-place.
        Silently ignores connection errors.
        """
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
        callback: Callable[[int, int], None] | None = None,
    ) -> pathlib.Path:
        """Download the file to a local path."""
        if not output:
            output = CACHEPATH / self.name
        return await self.client._download_file(self, output, callback=callback)

    async def fetch_size(self) -> int:
        """Fetch the remote file size and update the local record.

        Makes a HEAD request (falling back to GET with a Range header)
        to determine the Content-Length.

        Returns
        -------
        int
            The file size in bytes, or 0 if the size could not be determined.
        """
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
    _formatter: Callable[[str], dict[str, Any]] | None = PrivateAttr(default=None)

    def __init__(
        self,
        record: ConjuntoDados,
        dataset: BaseRemoteDataset,
        formatter: Callable | None = None,
    ):
        """Initialize the Group with a dataset record and optional formatter.

        Parameters
        ----------
        record : ConjuntoDados
            The API response record for this group.
        dataset : BaseRemoteDataset
            The parent dataset this group belongs to.
        formatter : Callable, optional
            A callable that extracts metadata from filenames.
        """
        super().__init__(
            record=record,
            dataset=dataset,  # type: ignore[call-arg]
        )
        self._formatter = formatter

    def __repr__(self):
        """Return the group name as its string representation."""
        return self.name

    @property
    def name(self) -> str:
        """Return the group name, resolved through dataset aliases.

        Returns
        -------
        str
            The alias for the group slug if defined, otherwise the raw slug.
        """
        slug = self.record.slug
        aliases = getattr(self.dataset, "group_aliases", {})
        return aliases.get(slug, slug)

    @property
    def long_name(self) -> str:
        """Return the group title.

        Returns
        -------
        str
            The title of the underlying API record.
        """
        return self.record.title

    @property
    def description(self) -> str:
        """Return an empty description for the group.

        Returns
        -------
        str
            An empty string.
        """
        return ""

    async def _fetch_files(self) -> list[BaseRemoteFile]:
        """Build File objects from the underlying resources."""
        entries: list[tuple[str, Any, dict]] = []
        for recurso in self.record.resources:
            filename = recurso.file_name or recurso.url.split("/")[-1].split("?")[0]
            if filename.lower().endswith(".pdf") or filename.startswith("get_"):
                continue
            metadata = {}
            if self._formatter:
                try:
                    metadata = self._formatter(filename)
                except NotImplementedError:
                    pass
            entries.append((filename, recurso, metadata))

        entries = _dedup_entries(entries)

        files: list[BaseRemoteFile] = []
        for _, recurso, metadata in entries:
            file = File(
                record=recurso,
                dataset=self.dataset,
                group=self,
                path=recurso.url,
                _metadata=metadata,
            )
            files.append(file)
        return files


class Dataset(BaseRemoteDataset):
    """A health dataset available through dados.gov.br.

    Subclasses define a list of API dataset IDs and an optional
    :meth:`formatter` that extracts metadata from file names.
    """

    ids: list[str] = []
    client: DadosGov
    group_aliases: dict[str, str] = {}

    def __repr__(self):
        """Return the dataset name as its string representation."""
        return self.name

    @abstractmethod
    def formatter(self, filename: str) -> dict[str, Any]:
        """Extract structured metadata from a filename."""
        pass

    async def _fetch_content(self) -> list[Group]:
        """Fetch all groups belonging to this dataset."""
        items: list[Group] = []
        client: DadosGov = self.client
        if self.ids:
            for group_id in self.ids:
                record = await client.get_dataset(group_id)
                items.append(
                    Group(record=record, dataset=self, formatter=self.formatter)
                )
        return items
