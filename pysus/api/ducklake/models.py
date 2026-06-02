"""Application-level models for DuckLake remote resources.

Wraps catalog ORM records into BaseRemoteFile, BaseRemoteDataset,
and BaseRemoteGroup interfaces used by the rest of PySUS.
"""

import hashlib
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Union

from anyio import to_thread
from pydantic import Field, PrivateAttr
from pysus import CACHEPATH
from pysus.api.models import (
    BaseRemoteDataset,
    BaseRemoteFile,
    BaseRemoteGroup,
)
from sqlalchemy.orm import joinedload, sessionmaker

from .catalog import CatalogDataset, CatalogFile, DatasetGroup

if TYPE_CHECKING:
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

        actual_hash = await to_thread.run_sync(_calculate)
        return actual_hash == self.sha256


class DuckDataset(BaseRemoteDataset):
    """A dataset from the DuckLake catalog, containing groups and files.

    Each dataset manages its own DuckDB engine connected to a
    per-dataset catalog file (``catalog_<name>.db``).

    Parameters
    ----------
    record : CatalogDataset
        The underlying ORM record.
    client : BaseRemoteClient
        The parent client instance.
    """

    record: CatalogDataset = Field(exclude=True)
    client: "DuckLake" = Field(exclude=True)

    _engine: Any = PrivateAttr(default=None)
    _Session: Any = PrivateAttr(default=None)
    _cache_dir: Path = PrivateAttr()
    _catalog_local: Path = PrivateAttr()

    def __init__(self, **data):
        super().__init__(**data)
        self._cache_dir = Path(CACHEPATH) / "ducklake"
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._catalog_local = self._cache_dir / f"catalog_{self.record.name.lower()}.db"

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

    async def connect(self, force: bool = False):
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

        await self.client.download_dataset_catalog(self)  # type: ignore[arg-type]
        self._engine = await to_thread.run_sync(
            lambda: self.client._setup_engine(self._catalog_local)
        )
        self._Session = sessionmaker(bind=self._engine)

    async def close(self):
        """Dispose the engine, uploading the catalog if authenticated."""
        if self._engine:
            await to_thread.run_sync(self._engine.dispose)
            self._engine = None
            self._Session = None

            if self.client.credentials:
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
                f"catalog_{self.record.name.lower()}.db",
            )

        await to_thread.run_sync(_upload)

    async def _fetch_content(self) -> list[Union["DuckGroup", File]]:
        """Fetch groups and files belonging to this dataset."""
        if not self._Session:
            await self.connect()

        def _fetch():
            with self._Session() as session:
                dataset = (
                    session.query(CatalogDataset)
                    .options(
                        joinedload(CatalogDataset.groups).joinedload(
                            DatasetGroup.files
                        ),
                        joinedload(CatalogDataset.files),
                    )
                    .filter(CatalogDataset.name == self.record.name)
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
                        path=f.path,
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
                path=f.path,
                record=f,
                group=self,
                dataset=self.dataset,
            )
            for f in self.record.files
        ]
        return files



