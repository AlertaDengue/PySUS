"""Application-level models for DuckLake remote resources.

Wraps catalog ORM records into BaseRemoteFile, BaseRemoteDataset,
and BaseRemoteGroup interfaces used by the rest of PySUS.
"""

import hashlib
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

from anyio import to_thread
from pydantic import Field, PrivateAttr
from pysus import CACHEPATH
from pysus.api.ducklake.catalog.orm.dataset import Dataset
from pysus.api.ducklake.catalog.orm.dataset import File as CatalogFile
from pysus.api.ducklake.catalog.orm.dataset import Group
from pysus.api.models import BaseRemoteDataset, BaseRemoteFile, BaseRemoteGroup
from sqlalchemy.orm import contains_eager, joinedload, sessionmaker

if TYPE_CHECKING:  # pragma: no cover
    from .client import DuckLake


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
    group: Optional["DuckGroup"] = Field(default=None, exclude=True)

    def __init__(self, **data: Any) -> None:
        record = data.pop("record")
        group = data.pop("group", None)
        super().__init__(
            path=Path(record.path),
            type=record.type or "remote",
            record=record,  # type: ignore[call-arg]
            group=group,
            **data,
        )

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

        actual_hash = await to_thread.run_sync(_calculate)
        return actual_hash == self.sha256


class DuckDataset(BaseRemoteDataset):
    """A dataset from the DuckLake catalog, containing groups and files.

    Each dataset manages its own DuckDB engine connected to a
    per-dataset catalog file (``catalog_<name>.db``).

    Parameters
    ----------
    record : Dataset
        The underlying ORM record.
    client : BaseRemoteClient
        The parent client instance.
    """

    record: Dataset = Field(exclude=True)
    client: "DuckLake" = Field(exclude=True)

    _engine: Any = PrivateAttr(default=None)
    _Session: Any = PrivateAttr(default=None)

    def __init__(self, **data) -> None:
        super().__init__(**data)
        self._cache_dir: Path = Path(CACHEPATH) / "ducklake"
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._catalog_name: str = f"catalog_{self.record.name.lower()}.duckdb"
        self._catalog_local: Path = self._cache_dir / self._catalog_name

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
        return self.record.name  # type: ignore

    @property
    def long_name(self) -> str:
        """Return the human-readable name of the dataset.

        Returns
        -------
        str
            The dataset display name, falling back to the short name.
        """
        return ""  # TODO:

    @property
    def description(self) -> str:
        """Return the description of the dataset.

        Returns
        -------
        str
            The dataset description, or an empty string if unavailable.
        """
        return ""  # TODO:

    @property
    def catalog_path(self) -> Path:
        """Return the local path to the downloaded catalog database.

        Returns
        -------
        Path
            Filesystem path to the local catalog database file.
        """
        return self._catalog_local

    async def connect(
        self,
        force: bool = False,
        callback: Callable[[int, int], None] | None = None,
    ) -> None:
        """Connect to the catalog, downloading it first if necessary.

        Parameters
        ----------
        force : bool, optional
            Whether to re-download and re-connect even if already connected.
        """
        if self._engine and not force:
            if not self._Session:
                self._Session = sessionmaker(bind=self._engine)
            return

        if self not in self.client._datasets:
            self.client._datasets.append(self)

        await self.client._download(
            f"public/{self._catalog_name}",
            self._catalog_local,
            callback=callback,
        )
        self._engine = await to_thread.run_sync(
            lambda: self.client._setup_engine(self._catalog_local)
        )
        self._Session = sessionmaker(bind=self._engine)

    async def close(self, update_catalog: bool = False):
        """Dispose the engine, optionally uploading the per-dataset catalog.

        Parameters
        ----------
        update_catalog : bool, optional
            Whether to upload the per-dataset catalog to remote storage.
            Requires the parent client to be authenticated.
        """
        if self._engine:
            await to_thread.run_sync(self._engine.dispose)
            self._engine = None
            self._Session = None

            if update_catalog and self.client._is_authenticated:
                await self._upload_catalog()

    async def _upload_catalog(self):
        """Upload the per-dataset catalog to remote storage."""
        if not self.client.credentials:
            raise PermissionError(
                "Admin credentials required to upload catalog.",
            )

        def _upload():
            self.client._s3_client.upload_file(
                str(self._catalog_local),
                self.client.bucket,
                f"catalog_{self.record.name.lower()}.duckdb",
            )

        await to_thread.run_sync(_upload)

    async def query(
        self,
        group: str | None = None,
        state: str | None = None,
        year: int | None = None,
        month: int | None = None,
    ) -> list[File]:
        """Filter files in this dataset's catalog by group, state, year, month.

        Parameters
        ----------
        group : str, optional
            Group name pattern to filter by (case-insensitive ILIKE).
        state : str, optional
            Two-letter state code to filter by.
        year : int, optional
            Year to filter by.
        month : int, optional
            Month to filter by.

        Returns
        -------
        list[File]
            List of matching file objects.
        """
        if not self._Session:
            await self.connect()

        def _query() -> list[CatalogFile]:
            with self._Session() as session:
                q = session.query(CatalogFile).options(
                    joinedload(CatalogFile.group),
                    joinedload(CatalogFile.dataset),
                )
                if group:
                    q = (
                        q.join(CatalogFile.group)
                        .options(contains_eager(CatalogFile.group))
                        .filter(Group.name.ilike(group))
                    )
                if state:
                    q = q.filter(CatalogFile.state == state.upper())
                if year:
                    q = q.filter(CatalogFile.year == year)
                if month:
                    q = q.filter(CatalogFile.month == month)
                results = q.all()
                session.expunge_all()
                return results

        records: list[CatalogFile] = await to_thread.run_sync(_query)
        return [File(record=r, dataset=self) for r in records]

    async def _fetch_content(self) -> list[Union["DuckGroup", File]]:
        """Fetch groups and files belonging to this dataset."""
        if not self._Session:
            await self.connect()

        def _fetch():
            with self._Session() as session:
                dataset = (
                    session.query(Dataset)
                    .options(
                        joinedload(Dataset.groups).joinedload(Group.files),
                        joinedload(Dataset.files),
                    )
                    .filter(Dataset.name == self.record.name)
                    .first()
                )
                if not dataset:
                    return [], []
                session.expunge_all()
                return dataset.groups, dataset.files

        groups, files = await to_thread.run_sync(_fetch)

        items: list[Union["DuckGroup", File]] = []

        if groups:
            items.extend([DuckGroup(record=g, dataset=self) for g in groups])

        if files:
            items.extend(
                [
                    File(
                        record=f,
                        dataset=self,
                    )
                    for f in files
                ]
            )

        return items


class DuckGroup(BaseRemoteGroup):
    """A group of related files within a DuckLake dataset.

    Parameters
    ----------
    record : Group
        The underlying ORM record.
    dataset : DuckDataset
        The parent dataset instance.
    """

    record: Group = Field(exclude=True)
    dataset: DuckDataset = Field(exclude=True)

    @property
    def name(self) -> str:
        """Return the short name of the group.

        Returns
        -------
        str
            The group short name.
        """
        return self.record.name  # type: ignore

    @property
    def long_name(self) -> str:
        """Return the human-readable name of the group.

        Returns
        -------
        str
            The group display name, falling back to the short name.
        """
        return self.record.long_name or self.name  # type: ignore

    @property
    def description(self) -> str:
        """Return the description of the group.

        Returns
        -------
        str
            The group description, or an empty string if unavailable.
        """
        return self.record.description  # type: ignore

    async def _fetch_files(self) -> list[BaseRemoteFile]:
        """Fetch the list of files belonging to this group."""
        files: list[BaseRemoteFile] = [
            File(
                record=f,
                group=self,
                dataset=self.dataset,
            )
            for f in self.record.files
        ]
        return files
