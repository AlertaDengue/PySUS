"""Inventory collection: snapshot every file visible on each client.

The collectors reduce FTP, DadosGov and DuckLake listings into
:class:`~pysus.management.records.FileRecord` objects. Snapshots are
persisted locally (JSON) so consecutive runs can diff against the previous
state without re-listing.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pysus import CACHEPATH

from .records import FileRecord, SnapshotDiff

if TYPE_CHECKING:  # pragma: no cover
    from pysus.api.client import PySUS
    from pysus.api.models import BaseRemoteFile

SNAPSHOT_DIR: Path = Path(CACHEPATH) / "management" / "inventory"

_ORIGIN_TO_CLIENT = {
    "ftp": "ftp",
    "dadosgov": "dadosgov",
    "ducklake": "ducklake",
    "saude": "saude",
}


class Inventory:
    """Collect and persist file listings from all three clients."""

    def __init__(self, pysus: PySUS, snapshot_dir: Path | None = None):
        self.pysus = pysus
        self.snapshot_dir = snapshot_dir or SNAPSHOT_DIR
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # collection
    # ------------------------------------------------------------------
    async def collect(
        self,
        origin: str,
        datasets: list[str] | None = None,
        dadosgov_token: str | None = None,
    ) -> list[FileRecord]:
        """Collect the file listing of *origin* (``ftp``, ``dadosgov``,
        ``ducklake`` or ``saude``), optionally restricted to *datasets*
        (canonical uppercase names)."""
        origin = origin.strip().lower()
        if origin == "ftp":
            return await self._collect_ftp(datasets)
        if origin == "dadosgov":
            return await self._collect_dadosgov(datasets, token=dadosgov_token)
        if origin == "ducklake":
            return await self._collect_ducklake(datasets)
        if origin == "saude":
            return await self._collect_saude(datasets)
        raise ValueError(f"Unknown origin: {origin!r}")

    async def collect_all(
        self,
        datasets: list[str] | None = None,
        dadosgov_token: str | None = None,
    ) -> dict[str, list[FileRecord]]:
        """Collect from every client, returning ``{origin: records}``."""
        return {
            "ducklake": await self._collect_ducklake(datasets),
            "ftp": await self._collect_ftp(datasets),
            "dadosgov": await self._collect_dadosgov(
                datasets, token=dadosgov_token
            ),
            "saude": await self._collect_saude(datasets),
        }

    async def _collect_ftp(
        self, datasets: list[str] | None = None
    ) -> list[FileRecord]:
        client = await self.pysus.get_ftp()
        records: list[FileRecord] = []
        for dataset in await client.datasets():
            if datasets and dataset.name.upper() not in datasets:
                continue
            for item in await dataset.content:
                records.extend(await self._walk_ftp_item(item))
        return records

    async def _walk_ftp_item(self, item: Any) -> list[FileRecord]:
        from pysus.api.ftp.models import Directory
        from pysus.api.ftp.models import File as FTPFile
        from pysus.api.models import BaseRemoteGroup

        if isinstance(item, FTPFile):
            record = FileRecord(
                origin="ftp",
                dataset=item.dataset.name,
                name=item.basename,
                path=str(item.path),
                size=item.size,
                modified=_safe_modify(item),
                group=getattr(item.group, "name", None),
                year=item.year,
                month=item.month,
                state=item.state,
                file=item,
            )
            return [record]

        if isinstance(item, BaseRemoteGroup):
            records: list[FileRecord] = []
            for file in await item.files:
                records.extend(await self._walk_ftp_item(file))
            return records

        if isinstance(item, Directory):
            dir_records: list[FileRecord] = []
            for child in await item.content:
                dir_records.extend(await self._walk_ftp_item(child))
            return dir_records

        return []

    async def _collect_dadosgov(
        self,
        datasets: list[str] | None = None,
        token: str | None = None,
    ) -> list[FileRecord]:
        from pysus.api.models import BaseRemoteGroup

        client = await self.pysus.get_dadosgov(token)
        records: list[FileRecord] = []
        for dataset in await client.datasets():
            if datasets and dataset.name.upper() not in datasets:
                continue
            for group in await dataset.content:
                if not isinstance(group, BaseRemoteGroup):
                    continue
                for file in await group.files:
                    records.append(
                        FileRecord(
                            origin="dadosgov",
                            dataset=dataset.name,
                            name=file.basename,
                            path=str(file.path),
                            size=file.size,
                            modified=_safe_modify(file),
                            group=getattr(group, "name", None),
                            year=file.year,
                            month=file.month,
                            state=file.state,
                            file=file,
                        )
                    )
        return records

    async def _collect_ducklake(
        self, datasets: list[str] | None = None
    ) -> list[FileRecord]:
        client = await self.pysus.get_ducklake()
        records: list[FileRecord] = []
        for dataset in await client.datasets():
            if datasets and dataset.name.upper() not in datasets:
                continue
            for file in await dataset.query():
                record = file.record
                records.append(
                    FileRecord(
                        origin="ducklake",
                        dataset=dataset.name,
                        name=file.basename,
                        path=str(file.path),
                        size=file.size,
                        modified=record.modified,
                        group=(record.group.name if record.group else None),
                        year=record.year,
                        month=record.month,
                        state=record.state,
                        sha256=record.sha256,
                        rows=record.rows,
                        source_path=record.origin_path,
                        source_size=record.origin_size,
                        source_modified=record.origin_modified,
                        file=file,
                    )
                )
        return records

    async def _collect_saude(
        self, datasets: list[str] | None = None
    ) -> list[FileRecord]:
        """Collect from the OpenDataSUS (dadosabertos.saude.gov.br) client.

        Content items are either:

        - ``SaudeGroup`` — CKAN packages whose files are walked
          recursively (like DadosGov);
        - ``SaudeEndpointFile`` — DEMAS REST endpoints, each turned
          into a single ``FileRecord`` with ``format="jsonl"``.
        """
        from pysus.api.models import BaseRemoteGroup
        from pysus.api.saude.models import SaudeEndpointFile

        client = await self.pysus.get_saude()
        records: list[FileRecord] = []
        for dataset in await client.datasets():
            if datasets and dataset.name.upper() not in datasets:
                continue
            for item in await dataset.content:
                if isinstance(item, SaudeEndpointFile):
                    # DEMAS endpoint file → single record
                    ep_name = item.record.path.strip("/").replace("/", "_")
                    records.append(
                        FileRecord(
                            origin="saude",
                            dataset=dataset.name,
                            name=f"{ep_name}.jsonl",
                            path=str(item.path),
                            size=item.size,
                            modified=_safe_modify(item),
                            year=item.year,
                            month=item.month,
                            state=item.state,
                            format="jsonl",
                            file=item,
                        )
                    )
                elif isinstance(item, BaseRemoteGroup):
                    # CKAN group → walk its files
                    for file in await item.files:
                        records.append(
                            FileRecord(
                                origin="saude",
                                dataset=dataset.name,
                                name=file.basename,
                                path=str(file.path),
                                size=file.size,
                                modified=_safe_modify(file),
                                group=getattr(item, "name", None),
                                year=file.year,
                                month=file.month,
                                state=file.state,
                                file=file,
                            )
                        )
        return records

    # ------------------------------------------------------------------
    # snapshot persistence
    # ------------------------------------------------------------------
    def _snapshot_path(self, origin: str) -> Path:
        return self.snapshot_dir / f"{origin.lower()}.json"

    def save_snapshot(self, origin: str, records: list[FileRecord]) -> Path:
        """Persist *records* as the latest snapshot for *origin*."""
        path = self._snapshot_path(origin)
        payload = {
            "origin": origin.lower(),
            "captured_at": datetime.now().isoformat(timespec="seconds"),
            "count": len(records),
            "records": [r.to_dict() for r in records],
        }
        path.write_text(json.dumps(payload, indent=2, default=str))
        return path

    def load_snapshot(self, origin: str) -> list[FileRecord] | None:
        """Load the previous snapshot for *origin*, if any."""
        path = self._snapshot_path(origin)
        if not path.exists():
            return None
        try:
            payload: dict[str, Any] = json.loads(path.read_text())
            return [FileRecord.from_dict(r) for r in payload["records"]]
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            return None

    def diff(
        self,
        previous: list[FileRecord] | None,
        current: list[FileRecord],
        origin: str,
    ) -> SnapshotDiff:
        """Compare a previous snapshot with the current listing."""
        prev_by_path = {r.path: r for r in (previous or [])}
        curr_by_path = {r.path: r for r in current}

        added = [r for p, r in curr_by_path.items() if p not in prev_by_path]
        removed = [r for p, r in prev_by_path.items() if p not in curr_by_path]
        changed = [
            (prev_by_path[p], curr_by_path[p])
            for p in prev_by_path.keys() & curr_by_path.keys()
            if _record_changed(prev_by_path[p], curr_by_path[p])
        ]

        return SnapshotDiff(
            origin=origin,
            added=added,
            removed=removed,
            changed=changed,
        )


def _safe_modify(file: BaseRemoteFile) -> datetime | None:
    try:
        return file.modify
    except (ValueError, AttributeError):
        return None


def _record_changed(previous: FileRecord, current: FileRecord) -> bool:
    return previous.size != current.size or _safe_iso(
        previous.modified
    ) != _safe_iso(current.modified)


def _safe_iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None
