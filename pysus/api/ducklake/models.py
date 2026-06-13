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
from pydantic import Field
from pysus import CACHEPATH
from .catalog.adapters import DatasetAdapter
from .catalog.orm.default import Dataset
from .catalog.orm.dataset import (
    File as CatalogFile,
    Group,
)
from pysus.api.models import BaseRemoteDataset, BaseRemoteFile, BaseRemoteGroup
from sqlalchemy import select, orm

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

        return await self.client.download(
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
    record: "Dataset" = Field(exclude=True)
    client: "DuckLake" = Field(exclude=True)
    adapter: "DatasetAdapter" = Field(exclude=True)
    update_on_close: bool = Field(default=False, exclude=True)

    def __init__(self, **data) -> None:
        super().__init__(**data)

    def __repr__(self) -> str:
        return self.name.upper()

    @property
    def name(self) -> str:
        return str(self.record.name)

    @property
    def long_name(self) -> str:
        return str(self.record.long_name)

    @property
    def description(self) -> str:
        return str(self.record.description)

    async def connect(
        self,
        force: bool = False,
    ) -> None:
        if self not in self.client._datasets:
            self.client._datasets.append(self)

        await self.adapter.connect(force=force)

    async def close(self, update_catalog: bool | None = None):
        should_update = (
            self.update_on_close if update_catalog is None else update_catalog
        )
        await self.adapter.close(update=should_update)

    async def query(
        self,
        group: str | None = None,
        state: str | None = None,
        year: int | None = None,
        month: int | None = None,
    ) -> list[File]:
        def _query() -> list[CatalogFile]:
            with self.adapter.get_session() as session:
                stmt = select(CatalogFile).filter(
                    CatalogFile.dataset_id == self.record.id,
                )

                if group:
                    stmt = (
                        stmt.join(CatalogFile.group)
                        .options(orm.contains_eager(CatalogFile.group))
                        .filter(Group.name.ilike(group))
                    )
                else:
                    stmt = stmt.options(orm.joinedload(CatalogFile.group))

                if state:
                    stmt = stmt.filter(CatalogFile.state == state.upper())
                if year:
                    stmt = stmt.filter(CatalogFile.year == year)
                if month:
                    stmt = stmt.filter(CatalogFile.month == month)

                results = session.scalars(stmt).all()
                session.expunge_all()
                return list(results)

        async with self.adapter:
            records: list[CatalogFile] = await to_thread.run_sync(_query)
            return [File(record=r, dataset=self) for r in records]

    async def _fetch_content(self) -> list[Union["DuckGroup", File]]:
        def _fetch():
            with self.adapter.get_session() as session:
                stmt = (
                    select(Group)
                    .options(orm.joinedload(Group.files))
                    .filter(Group.dataset_id == self.record.id)
                )
                groups = session.scalars(stmt).all()

                ungrouped = session.scalars(
                    select(CatalogFile).filter(
                        CatalogFile.dataset_id == self.record.id,
                        CatalogFile.group_id.is_(None),
                    )
                ).all()

                session.expunge_all()
                return list(groups), list(ungrouped)

        async with self.adapter:
            groups, files = await to_thread.run_sync(_fetch)

            items: list[Union[DuckGroup, File]] = []

            if groups:
                items.extend([DuckGroup(record=g, dataset=self) for g in groups])

            if files:
                items.extend([File(record=f, dataset=self) for f in files])

            return items

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close(update_catalog=None)


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
        return str(self.record.name)

    @property
    def long_name(self) -> str:
        """Return the human-readable name of the group.

        Returns
        -------
        str
            The group display name, falling back to the short name.
        """
        return str(self.record.long_name)

    @property
    def description(self) -> str:
        """Return the description of the group.

        Returns
        -------
        str
            The group description, or an empty string if unavailable.
        """
        return str(self.record.description)

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
