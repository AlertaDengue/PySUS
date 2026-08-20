"""Normalized records shared by the management workflow.

These dataclasses are origin-agnostic: every client (FTP, DadosGov,
DuckLake/S3) is reduced to the same :class:`FileRecord` shape so that
tracking, comparison and catalog persistence operate uniformly.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

ORIGINS: tuple[str, ...] = ("ducklake", "ftp", "dadosgov", "saude")

#: Download resolution order: S3 parquet first, FTP second, DadosGov last
#: (only used when a file is not on S3 and requires the API token).
#: Saude is the 4th priority — a fallback after FTP and DadosGov.
DOWNLOAD_PRIORITY: tuple[str, ...] = ("ducklake", "ftp", "dadosgov", "saude")

_FORMAT_SUFFIXES = (".csv", ".json", ".xml", ".xlsx", ".xls", ".dbx")
_COMPRESSION_SUFFIXES = (".zip", ".gz", ".bz2", ".7z", ".rar")

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def canonical_dataset(name: str) -> str:
    """Return the canonical uppercase dataset name."""
    return name.strip().upper()


def canonical_group(name: str | None) -> str | None:
    """Return the canonical uppercase group code, if any."""
    if not name:
        return None
    return name.strip().upper() or None


def stem_of(name: str) -> str:
    """Extract a format-independent file stem.

    ``DENGBR25.csv.zip``, ``DENGBR25.json`` and ``DENGBR25.dbc`` all map to
    ``dengbr25``, making them comparable across clients despite different
    formats and compression.
    """
    stem = base_stem(name)
    return _NON_ALNUM_RE.sub("_", stem.lower()).strip("_")


def base_stem(name: str) -> str:
    """Return the format-independent, case-preserving stem of *name*.

    ``CHIKBR15.csv.zip`` → ``CHIKBR15``; ``DENGBR25.dbc`` → ``DENGBR25``;
    ``Mortalidade_Geral_2022_csv.zip`` → ``Mortalidade_Geral_2022``.

    This is the canonical base used to build S3 parquet keys, so format
    tokens (dotted or underscored) never leak into catalog paths.
    """
    stem = name.strip()

    while stem.lower().endswith(_COMPRESSION_SUFFIXES):
        stem = Path(stem).stem
    stem = Path(stem).stem

    for suffix in (
        ".csv",
        "_csv",
        ".json",
        "_json",
        ".xml",
        "_xml",
        ".xlsx",
        ".xls",
    ):
        if stem.lower().endswith(suffix):
            stem = stem[: -len(suffix)]
            break

    return stem


def parquet_key(name: str) -> str:
    """Return the canonical parquet filename for *name*.

    ``CHIKBR15.csv.zip`` → ``CHIKBR15.parquet`` (not ``CHIKBR15.csv.parquet``).
    """
    return f"{base_stem(name)}.parquet"


#: Placeholder used in S3 keys for attributes that are not present.
KEY_MISSING = "_"

#: State used in S3 keys for national (country-wide) files.
NATIONAL_STATE = "BR"

_KEY_SEGMENT_ORDER = ("group", "year", "month", "state")


def compose_s3_key(
    origin: str,
    dataset: str,
    name: str,
    group: str | None = None,
    year: int | None = None,
    month: int | None = None,
    state: str | None = None,
) -> str:
    """Build the hierarchical S3 key for a parquet artifact.

    The directory structure composes the file characteristics, making the
    bucket navigable by prefix::

        public/data/<origin>/<dataset>/<group>/<year>/<month>/<state>/<STEM>.parquet

    Missing attributes use the ``_`` placeholder; a missing state is
    interpreted as national (``BR``). Datasets with different metadata
    shapes (SINAN has no month, SIA has all four, PNI has no month/state)
    therefore keep a stable, predictable layout.
    """
    segments = {
        "group": canonical_group(group),
        "year": str(year) if year is not None else None,
        "month": f"{month:02d}" if month is not None else None,
        "state": (
            (state.strip().upper() or NATIONAL_STATE)
            if state
            else NATIONAL_STATE
        ),
    }
    dirs = [origin.strip().lower(), canonical_dataset(dataset).lower()]
    dirs.extend(segments[key] or KEY_MISSING for key in _KEY_SEGMENT_ORDER)
    return "/".join(["public/data", *dirs, parquet_key(name)])


def format_of(name: str) -> str:
    """Return the file format label (e.g. ``csv.zip``, ``dbc``, ``parquet``)."""
    lower = name.strip().lower()
    compression: list[str] = []
    while lower.endswith(_COMPRESSION_SUFFIXES):
        for suffix in _COMPRESSION_SUFFIXES:
            if lower.endswith(suffix):
                compression.insert(0, suffix.lstrip("."))
                lower = lower[: -len(suffix)]
                break
    suffix = Path(lower).suffix.lstrip(".")
    parts = ([suffix] if suffix else []) + compression
    return ".".join(parts) if parts else "unknown"


@dataclass(frozen=True)
class IdentityKey:
    """The logical identity of a file, independent of client and format."""

    dataset: str
    group: str | None
    year: int | None
    month: int | None
    state: str | None
    stem: str

    def as_tuple(self) -> tuple:
        return (
            self.dataset,
            self.group,
            self.year,
            self.month,
            self.state,
            self.stem,
        )


@dataclass
class FileRecord:
    """One physical artifact of a file on a specific client."""

    origin: str
    dataset: str
    name: str
    path: str
    size: int = 0
    modified: datetime | None = None
    group: str | None = None
    year: int | None = None
    month: int | None = None
    state: str | None = None
    format: str | None = None
    sha256: str | None = None
    rows: int | None = None
    source_path: str | None = None
    source_size: int | None = None
    source_modified: datetime | None = None
    file: Any = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        self.dataset = canonical_dataset(self.dataset)
        self.group = canonical_group(self.group)
        self.origin = self.origin.strip().lower()
        if self.format is None:
            self.format = format_of(self.name)
        if self.state is not None:
            self.state = self.state.strip().upper() or None

    @property
    def stem(self) -> str:
        return stem_of(self.name)

    def identity_key(self) -> IdentityKey:
        """Build the logical identity key for this record."""
        return IdentityKey(
            dataset=self.dataset,
            group=self.group,
            year=self.year,
            month=self.month,
            state=self.state,
            stem=self.stem,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize the record for snapshot persistence."""
        return {
            "origin": self.origin,
            "dataset": self.dataset,
            "name": self.name,
            "path": self.path,
            "size": self.size,
            "modified": self.modified.isoformat() if self.modified else None,
            "group": self.group,
            "year": self.year,
            "month": self.month,
            "state": self.state,
            "format": self.format,
            "sha256": self.sha256,
            "rows": self.rows,
            "source_path": self.source_path,
            "source_size": self.source_size,
            "source_modified": (
                self.source_modified.isoformat()
                if self.source_modified
                else None
            ),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FileRecord:
        """Rehydrate a record from a serialized snapshot."""
        modified = data.get("modified")
        source_modified = data.get("source_modified")
        return cls(
            origin=data["origin"],
            dataset=data["dataset"],
            name=data["name"],
            path=data["path"],
            size=data.get("size", 0),
            modified=datetime.fromisoformat(modified) if modified else None,
            group=data.get("group"),
            year=data.get("year"),
            month=data.get("month"),
            state=data.get("state"),
            format=data.get("format"),
            sha256=data.get("sha256"),
            rows=data.get("rows"),
            source_path=data.get("source_path"),
            source_size=data.get("source_size"),
            source_modified=(
                datetime.fromisoformat(source_modified)
                if source_modified
                else None
            ),
        )


@dataclass
class FileComparison:
    """Comparison of one logical file across all clients."""

    key: IdentityKey
    records: list[FileRecord] = field(default_factory=list)

    @property
    def origins(self) -> set[str]:
        return {r.origin for r in self.records}

    @property
    def formats(self) -> set[str]:
        return {r.format or "unknown" for r in self.records}

    def by_origin(self, origin: str) -> list[FileRecord]:
        return [r for r in self.records if r.origin == origin.lower()]

    def _pick(self, origin: str) -> FileRecord | None:
        records = self.by_origin(origin)
        if not records:
            return None
        return max(
            records,
            key=lambda r: (r.modified or datetime.min, r.size),
        )

    def best_record(
        self, priorities: tuple[str, ...] = DOWNLOAD_PRIORITY
    ) -> FileRecord | None:
        """Return the first available record following the priority order."""
        for origin in priorities:
            record = self._pick(origin)
            if record:
                return record
        return None

    @property
    def is_on_s3(self) -> bool:
        return "ducklake" in self.origins

    @property
    def only_on_dadosgov(self) -> bool:
        return self.origins == {"dadosgov"}

    @property
    def needs_token(self) -> bool:
        return self.only_on_dadosgov and "ftp" not in self.origins

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": dict(
                zip(
                    ("dataset", "group", "year", "month", "state", "stem"),
                    self.key.as_tuple(),
                )
            ),
            "origins": sorted(self.origins),
            "formats": sorted(self.formats),
            "records": [r.to_dict() for r in self.records],
        }


@dataclass
class SnapshotDiff:
    """Difference between two snapshots of the same origin."""

    origin: str
    added: list[FileRecord] = field(default_factory=list)
    removed: list[FileRecord] = field(default_factory=list)
    changed: list[tuple[FileRecord, FileRecord]] = field(
        default_factory=list
    )  # (previous, current)

    @property
    def has_changes(self) -> bool:
        return bool(self.added or self.removed or self.changed)

    @property
    def changed_count(self) -> int:
        return len(self.added) + len(self.removed) + len(self.changed)


@dataclass
class SyncOutcome:
    """Outcome of a single file during a sync run."""

    key: IdentityKey
    origin: str
    status: str  # "skipped" | "uploaded" | "failed" | "needs_token"
    detail: str = ""


@dataclass
class SyncReport:
    """Aggregated result of a sync run."""

    outcomes: list[SyncOutcome] = field(default_factory=list)
    dataset: str | None = None

    @property
    def uploaded(self) -> list[SyncOutcome]:
        return [o for o in self.outcomes if o.status == "uploaded"]

    @property
    def skipped(self) -> list[SyncOutcome]:
        return [o for o in self.outcomes if o.status == "skipped"]

    @property
    def failed(self) -> list[SyncOutcome]:
        return [o for o in self.outcomes if o.status == "failed"]

    @property
    def needs_token(self) -> list[SyncOutcome]:
        return [o for o in self.outcomes if o.status == "needs_token"]

    def summary(self) -> dict[str, int]:
        return {
            "total": len(self.outcomes),
            "uploaded": len(self.uploaded),
            "skipped": len(self.skipped),
            "failed": len(self.failed),
            "needs_token": len(self.needs_token),
        }
