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
from pysus.api.models import BaseRemoteDataset, BaseRemoteFile, BaseRemoteGroup
from sqlalchemy import or_, orm, select

from .catalog.adapters import DatasetAdapter
from .catalog.orm.dataset import File as CatalogFile
from .catalog.orm.dataset import Group
from .catalog.orm.default import Dataset

if TYPE_CHECKING:
    from .client import DuckLake


class File(BaseRemoteFile):
    group: Optional["DuckGroup"] = Field(default=None, exclude=True)

    _record: CatalogFile = PrivateAttr()

    def __init__(self, **data: Any) -> None:
        record = data.pop("record")
        group = data.pop("group", None)

        super().__init__(
            path=Path(record.path),
            type=record.type or "remote",
            group=group,
            **data,
        )
        self._record = record

    @property
    def record(self) -> CatalogFile:
        return self._record

    @property
    def basename(self) -> str:
        return self.path.name

    @property
    def extension(self) -> str:
        return self.path.suffix

    @property
    def size(self) -> int:
        return self.record.size

    @property
    def modify(self) -> datetime:
        return self.record.modified

    @property
    def rows(self) -> int:
        return self.record.rows

    @property
    def sha256(self) -> str | None:
        return self.record.sha256

    async def _download(
        self,
        output: Path | None = None,
        callback: Callable[[int, int], None] | None = None,
    ) -> Path:
        if not output:
            output = CACHEPATH / self.name
        return await self.client.download(self, output, callback=callback)

    async def verify(self, path: Path) -> bool:
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
    border: "DatasetAdapter" = Field(exclude=True)
    update_on_close: bool = Field(default=False, exclude=True)

    def __init__(self, **data) -> None:
        if "adapter" in data and "border" not in data:
            data["border"] = data.pop("adapter")
        super().__init__(**data)

    def __str__(self) -> str:
        return self.record.name

    @property
    def adapter(self) -> "DatasetAdapter":
        return self.border

    @property
    def id(self) -> int:
        return int(self.adapter.dataset_id)

    @property
    def name(self) -> str:
        return str(self.record.name)

    @property
    def long_name(self) -> str:
        return str(self.record.long_name)

    @property
    def description(self) -> str:
        return str(self.record.description)

    async def connect(self, force: bool = False) -> None:
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
        group: str | list[str] | None = None,
        state: str | list[str] | None = None,
        year: int | list[int] | None = None,
        month: int | list[int] | None = None,
    ) -> list[File]:
        def _to_list(val: Any) -> list[Any] | None:
            if val is None:
                return None
            return val if isinstance(val, list) else [val]

        groups = _to_list(group)
        states = _to_list(state)
        years = _to_list(year)
        months = _to_list(month)

        def _query() -> list[CatalogFile]:
            with self.adapter.get_session() as session:
                stmt = select(CatalogFile).filter(
                    CatalogFile.dataset_id == self.id,
                )

                if groups:
                    stmt = (
                        stmt.join(CatalogFile.group)
                        .options(orm.contains_eager(CatalogFile.group))
                        .filter(or_(*[Group.name.ilike(g) for g in groups]))
                    )
                else:
                    stmt = stmt.options(orm.joinedload(CatalogFile.group))

                if states:
                    stmt = stmt.filter(
                        CatalogFile.state.in_([s.upper() for s in states])
                    )
                if years:
                    stmt = stmt.filter(CatalogFile.year.in_(years))
                if months:
                    stmt = stmt.filter(CatalogFile.month.in_(months))

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
                    .filter(Group.dataset_id == self.id)
                )
                groups = session.scalars(stmt).all()

                ungrouped = session.scalars(
                    select(CatalogFile).filter(
                        CatalogFile.dataset_id == self.id,
                        CatalogFile.group_id.is_(None),
                    )
                ).all()

                session.expunge_all()
                return list(groups), list(ungrouped)

        async with self.adapter:
            groups, files = await to_thread.run_sync(_fetch)
            items: list[DuckGroup | File] = []

            if groups:
                items.extend(
                    [DuckGroup(record=g, dataset=self) for g in groups]
                )
            if files:
                items.extend([File(record=f, dataset=self) for f in files])
            return items

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close(update_catalog=None)


class DuckGroup(BaseRemoteGroup):
    record: Group = Field(exclude=True)
    dataset: DuckDataset = Field(exclude=True)

    def __str__(self) -> str:
        return self.name

    @property
    def name(self) -> str:
        return str(self.record.name)

    @property
    def long_name(self) -> str:
        return str(self.record.long_name)

    @property
    def description(self) -> str:
        return str(self.record.description)

    async def _fetch_files(self) -> list[BaseRemoteFile]:
        files: list[BaseRemoteFile] = [
            File(record=f, group=self, dataset=self.dataset)
            for f in self.record.files
        ]
        return files
