"""Abstract model hierarchy for PySUS data access.

Provides abstract base classes for local and remote file handling, organized
in a layered hierarchy: BaseFile -> BaseLocalFile -> BaseTabularFile /
BaseCompressedFile for local files, and BaseFile -> BaseRemoteFile for remote
files, alongside BaseRemoteObject -> BaseRemoteGroup / BaseRemoteDataset /
BaseRemoteClient for remote data catalogs.
"""

from __future__ import annotations

import asyncio
import hashlib
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Callable, Sequence
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from anyio import to_thread
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from pysus import CACHEPATH
from tqdm.asyncio import tqdm

from .types import FileType, State

if TYPE_CHECKING:  # pragma: no cover
    from extensions import Parquet
    from pysus.api.metadata.models import Column


class BaseFile(BaseModel, ABC):
    """Abstract base for a single file, local or remote.

    Subclasses must implement *name*, *extension*, *size*, and *modify*.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
    )

    path: Path
    type: str | FileType

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the display name of the file."""

    @property
    def basename(self) -> str:
        """Return the file name from the path."""
        return self.path.name

    def __str__(self) -> str:
        """Return the file's basename as its string representation."""
        return self.basename

    @property
    @abstractmethod
    def extension(self) -> str:
        """Return the file extension string."""

    @property
    @abstractmethod
    def size(self) -> int:
        """Return the file size in bytes."""

    @property
    @abstractmethod
    def modify(self) -> datetime:
        """Return the last modification timestamp."""


class BaseLocalFile(BaseFile, ABC):
    """Abstract base for a file stored on the local filesystem.

    Subclasses must implement *load* and *stream*.
    """

    path: Path

    @property
    def name(self) -> str:
        """Return the file name from the path."""
        return self.path.name

    async def get_hash(
        self, algorithm: str = "sha256", chunk_size: int = 1024 * 1024
    ) -> str:
        """Compute the file's hash digest.

        Parameters
        ----------
        algorithm : str, optional
            The hash algorithm name (default ``"sha256"``).
        chunk_size : int, optional
            Read chunk size in bytes (default 1 MiB).

        Returns
        -------
        str
            The hex digest string.
        """

        def _compute_hash():
            """Compute the hash digest in a thread-safe manner."""
            hash_obj = hashlib.new(algorithm)
            with open(self.path, "rb") as f:
                while chunk := f.read(chunk_size):
                    hash_obj.update(chunk)
            return hash_obj.hexdigest()

        return await to_thread.run_sync(_compute_hash)

    @abstractmethod
    async def load(self) -> Any:
        """Load the entire file content into memory and return it."""

    @abstractmethod
    def stream(
        self,
        chunk_size: int = 10000,
    ) -> AsyncGenerator[Any, None]:
        """Yield chunks of the file content as an async generator."""

    @property
    def extension(self) -> str:
        """Return the file extension from the local path."""
        return self.path.suffix

    @property
    def size(self) -> int:
        """Return the file size in bytes from the local filesystem."""
        return self.path.stat().st_size

    @property
    def modify(self) -> datetime:
        """Return the last modification timestamp from the local filesystem."""
        return datetime.fromtimestamp(self.path.stat().st_mtime)


class BaseTabularFile(BaseLocalFile, ABC):
    """Abstract base for a local tabular file (e.g. CSV, Parquet).

    Subclasses must implement *columns*, *rows*, *load*, and *stream*.
    """

    @property
    @abstractmethod
    def columns(self) -> list[Column]:
        """Return the list of column metadata."""

    @property
    @abstractmethod
    def rows(self) -> int:
        """Return the number of data rows."""

    @abstractmethod
    async def load(self) -> pd.DataFrame:
        """Load the entire file into a pandas DataFrame."""

    @abstractmethod
    def stream(
        self,
        chunk_size: int = 10000,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """Yield pandas DataFrames in chunks as an async generator."""

    async def to_parquet(
        self,
        output_path: str | Path | None = None,
        chunk_size: int = 10000,
        callback: Callable[[int, int], None] | None = None,
    ) -> Parquet:
        """Convert the file to Parquet format.

        Parameters
        ----------
        output_path : str or Path, optional
            Destination path for the Parquet file. Defaults to the source
            path with a ``.parquet`` extension.
        chunk_size : int, optional
            Number of rows per streaming chunk (default 10 000).
        callback : Callable[[int, int], None], optional
            Function called after each chunk with
            ``(current_rows, total_rows)``.

        Returns
        -------
        Parquet
            The resulting Parquet wrapper object.
        """
        from pysus.api.extensions import ExtensionFactory, Parquet

        if output_path is None:
            output_path = self.path.with_suffix(".parquet")

        output_path = Path(output_path).expanduser().resolve()
        writer = None
        total_rows = self.rows
        current_rows = 0

        pbar = tqdm(
            desc=f"Converting {self.basename}",
            unit=" rows",
            unit_scale=True,
            total=total_rows,
        )

        try:
            try:
                async for chunk in self.stream(
                    chunk_size=chunk_size,
                ):
                    if chunk.empty:
                        continue

                    rows_in_chunk = len(chunk)
                    current_rows += rows_in_chunk

                    table = await to_thread.run_sync(
                        pa.Table.from_pandas,
                        chunk,
                    )

                    schema = table.schema
                    if any(pa.types.is_null(f.type) for f in schema):
                        new_fields = [
                            (
                                pa.field(f.name, pa.string(), nullable=True)
                                if pa.types.is_null(f.type)
                                else f
                            )
                            for f in schema
                        ]
                        table = table.cast(pa.schema(new_fields))

                    if writer is None:
                        writer = await to_thread.run_sync(
                            pq.ParquetWriter, output_path, table.schema
                        )

                    await to_thread.run_sync(writer.write_table, table)

                    pbar.update(rows_in_chunk)

                    if callback:
                        callback(current_rows, total_rows)

                    await asyncio.sleep(0)
            finally:
                if writer:
                    await to_thread.run_sync(writer.close)
                    writer = None
        finally:
            pbar.close()

        output = await ExtensionFactory.instantiate(output_path)
        if not isinstance(output, Parquet):
            raise ValueError(f"Could not parse {output} to Parquet")
        return output


class BaseCompressedFile(BaseLocalFile, ABC):
    """Abstract base for a compressed archive file (e.g. .zip, .gz).

    Subclasses must implement *list_members*, *open_member*, and *extract*.
    """

    @abstractmethod
    async def list_members(self) -> list[str]:
        """Return the list of member names inside the archive."""

    @abstractmethod
    async def open_member(self, member_name: str) -> Any:
        """Open and return a single archive member by name."""

    @abstractmethod
    async def extract(
        self,
        target_dir: Path = CACHEPATH,
    ) -> list[BaseLocalFile]:
        """Extract all members into *target_dir* and return the file objects."""

    async def stream(
        self,
        chunk_size: int | None = None,
    ) -> AsyncGenerator[Any, None]:
        """Yield each archive member as it is opened."""
        members = await self.list_members()
        for member in members:
            yield await self.open_member(member)
            await asyncio.sleep(0)


class SearchableMixin:
    """Mixin providing attribute-based filtering for remote objects."""

    def _matches(self, obj: Any, **kwargs) -> bool:
        """Return True if all *kwargs* attributes equal those on *obj*."""
        for key, value in kwargs.items():
            obj_value = getattr(obj, key, None)
            if obj_value != value:
                return False
        return True


class BaseRemoteFile(BaseFile, SearchableMixin, ABC):
    """Abstract base for a file stored on a remote server.

    Subclasses must implement *_download*.  *dataset* and *group* link back
    to the containing objects.
    """

    dataset: BaseRemoteDataset = Field(exclude=True)
    group: BaseRemoteGroup | None = Field(default=None, exclude=True)

    @property
    def name(self) -> str:
        """Return the basename as the display name."""
        return self.basename

    @property
    def client(self) -> BaseRemoteClient:
        """Return the remote client associated with this file."""
        return self.dataset.client

    @property
    def year(self) -> int | None:
        """Return the year associated with the file, or None."""
        return None

    @property
    def month(self) -> int | None:
        """Return the month associated with the file, or None."""
        return None

    @property
    def state(self) -> State | None:
        """Return the state associated with the file, or None."""
        return None

    @abstractmethod
    async def _download(
        self,
        output: Path | None = None,
        callback: Callable[[int, int], None] | None = None,
    ) -> Path:
        """Download the file to *output* and return the local path.

        Subclasses implement the actual transfer logic.
        """

    async def download(
        self,
        output: str | Path | None = None,
        callback: Callable[[int, int], None] | None = None,
    ) -> BaseLocalFile:
        """Download the remote file to a local cache or *output* path.

        Return the instantiated local file wrapper.
        """
        from pysus.api.extensions import ExtensionFactory

        if output is None:
            cache_dir = Path(CACHEPATH)
            cache_dir.mkdir(parents=True, exist_ok=True)
            dest = cache_dir / self.basename
        else:
            output_path = Path(output).expanduser().resolve()
            if output_path.is_dir():
                output_path.mkdir(parents=True, exist_ok=True)
                dest = output_path / self.basename
            else:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                dest = output_path

        local_path = await self._download(output=dest, callback=callback)

        return await ExtensionFactory.instantiate(local_path)


class BaseRemoteObject(BaseModel, ABC):
    """Abstract base for a named remote entity with a description.

    Subclasses must implement *name*, *long_name*, and *description*.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __str__(self) -> str:
        """Return the short name as the string representation."""
        return self.name

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the short name of the remote entity."""

    @property
    @abstractmethod
    def long_name(self) -> str:
        """Return the long / human-readable name."""

    @property
    @abstractmethod
    def description(self) -> str:
        """Return a textual description of the entity."""


class BaseRemoteGroup(BaseRemoteObject, SearchableMixin, ABC):
    """Abstract base for a named group of remote files within a dataset.

    Subclasses must implement *_fetch_files*.
    """

    dataset: BaseRemoteDataset = Field(exclude=True)
    _files: list[BaseRemoteFile] | None = PrivateAttr(default=None)

    @property
    def parent(self) -> BaseRemoteDataset:
        """Return the parent dataset."""
        return self.dataset

    @abstractmethod
    async def _fetch_files(self) -> list[BaseRemoteFile]:
        """Fetch and return the list of files in this group."""

    @property
    async def files(self) -> list[BaseRemoteFile]:
        """Return all files in this group, fetching them on first access."""
        if self._files is None:
            self._files = await self._fetch_files()
        return self._files

    async def search(self, **kwargs) -> list[BaseRemoteFile]:
        """Filter files in this group by attribute *kwargs*.

        Return matching file objects.
        """
        all_files = await self.files
        if not kwargs:
            return all_files
        return [f for f in all_files if self._matches(f, **kwargs)]


class BaseRemoteDataset(BaseRemoteObject, SearchableMixin, ABC):
    """Abstract base for a dataset containing groups and/or files.

    Subclasses must implement *_fetch_content*.
    """

    client: BaseRemoteClient = Field(exclude=True)
    group_definitions: dict[str, str] = {}
    _content: Sequence[BaseRemoteGroup | BaseRemoteFile] | None = PrivateAttr(
        default=None
    )

    @abstractmethod
    async def _fetch_content(
        self,
    ) -> Sequence[BaseRemoteGroup | BaseRemoteFile]:
        """Fetch and return the top-level content (groups and files)."""

    @property
    async def content(
        self,
    ) -> Sequence[BaseRemoteGroup | BaseRemoteFile]:
        """Return the dataset content, fetching on first access."""
        if self._content is None:
            self._content = await self._fetch_content()

        return self._content

    async def search(self, **kwargs) -> list[BaseRemoteFile]:
        """Recursively search groups and files by attribute *kwargs*.

        Return matching file objects.
        """
        contents = await self.content

        matches = []
        for item in contents:
            if isinstance(item, BaseRemoteGroup):
                group_matches = await item.search(**kwargs)
                matches.extend(group_matches)
            elif isinstance(item, BaseRemoteFile):
                if self._matches(item, **kwargs):
                    matches.append(item)

        return matches


class BaseRemoteClient(BaseRemoteObject, ABC):
    """Abstract base for a remote API client (e.g. FTP, HTTP).

    Subclasses must implement *connect*, *close*, *login*, *datasets*, and
    *_download_file*.
    """

    @abstractmethod
    async def connect(self) -> None:
        """Establish a connection to the remote server."""

    @abstractmethod
    async def close(self) -> None:
        """Close the connection to the remote server."""

    @abstractmethod
    async def login(self, **kwargs) -> None:
        """Authenticate with the remote server using *kwargs* credentials."""

    @abstractmethod
    async def datasets(self, **kwargs) -> list:
        """Return a list of available datasets matching *kwargs*."""

    @abstractmethod
    async def download(
        self,
        file: BaseRemoteFile,
        output: Path,
        callback: Callable[[int, int], None] | None = None,
    ) -> Path:
        """Download a single *file* to *output* and return the local path."""
