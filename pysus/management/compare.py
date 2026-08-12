"""Cross-client comparison of inventoried files.

Files with the same logical identity (dataset + group + year/month/state +
format-independent stem) are grouped into :class:`FileComparison` objects
that expose which origins carry the file, in which formats, and which
record should be preferred for download.

Content-level equivalence (same data, different bytes/format) is confirmed
by :func:`content_fingerprint`, which operates on decompressed/parsed
data — raw sizes can never be compared across formats (a ``csv.zip`` is
not byte-comparable to a ``parquet``).
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable

import pandas as pd

from .records import DOWNLOAD_PRIORITY, FileComparison, FileRecord, IdentityKey


class Comparator:
    """Group and compare :class:`FileRecord` objects across origins."""

    def __init__(
        self,
        priorities: tuple[str, ...] = DOWNLOAD_PRIORITY,
    ):
        self.priorities = priorities

    def compare(
        self,
        records: Iterable[FileRecord],
    ) -> list[FileComparison]:
        """Group records by logical identity.

        The grouping key is ``(dataset, year, stem)``. Group, month and
        state are deliberately excluded: they are encoded in the stem for
        state/month/group-level files (e.g. ``PAAC2408``), and the same
        logical file is known to carry different values per origin —
        FTP leaves ``state``/``group`` null on national files while
        DadosGov sets ``"BR"``, and legacy catalog rows have
        ``group_id NULL``. Those attributes are preserved per record and
        resolved onto the comparison key from the richest record. This is
        deliberately permissive: content fingerprints can later veto a
        wrong grouping.
        """
        groups: dict[tuple, list[FileRecord]] = {}
        order: list[tuple] = []
        for record in records:
            key = record.identity_key()
            group_key = (key.dataset, key.year, key.stem)
            if group_key not in groups:
                groups[group_key] = []
                order.append(group_key)
            groups[group_key].append(record)

        comparisons: list[FileComparison] = []

        def _pick(records: list[FileRecord], attr: str):
            return next(
                (
                    getattr(r, attr)
                    for r in records
                    if getattr(r, attr) is not None
                ),
                None,
            )

        for group_key in order:
            items = self._dedup_origin_formats(groups[group_key])
            dataset, year, stem = group_key

            comparisons.append(
                FileComparison(
                    key=IdentityKey(
                        dataset=dataset,
                        group=_pick(items, "group"),
                        year=year,
                        month=_pick(items, "month"),
                        state=_pick(items, "state"),
                        stem=stem,
                    ),
                    records=items,
                )
            )
        return comparisons

    @staticmethod
    def _dedup_origin_formats(
        records: list[FileRecord],
    ) -> list[FileRecord]:
        """Keep only one record per (origin, logical key).

        DadosGov publishes the same data as csv/json/xml triplets: when
        several records from the same origin share size-agnostic identity,
        prefer the format highest in ``FORMAT_PREFERENCE`` (csv first).
        """
        preferred: dict[str, FileRecord] = {}
        for record in records:
            fmt = (record.format or "").lower()
            current = preferred.get(record.origin)
            if current is None or _format_rank(fmt) < _format_rank(
                current.format or ""
            ):
                preferred[record.origin] = record
        return list(preferred.values())

    def pick(
        self,
        comparison: FileComparison,
    ) -> FileRecord | None:
        """Resolve the download source for a logical file.

        S3 (ducklake) first, then FTP, then DadosGov (which needs the API
        token). Within an origin the most recently modified record wins.
        """
        return comparison.best_record(priorities=self.priorities)


FORMAT_PREFERENCE: tuple[str, ...] = (
    "csv",
    "csv.zip",
    "json",
    "xml",
    "xlsx",
    "dbf",
    "dbc",
    "parquet",
)


def _format_rank(fmt: str) -> int:
    fmt = fmt.strip().lower()
    if not fmt or fmt == "unknown":
        return len(FORMAT_PREFERENCE) + 1
    try:
        return FORMAT_PREFERENCE.index(fmt)
    except ValueError:
        return len(FORMAT_PREFERENCE)


def content_fingerprint(
    frame: pd.DataFrame,
    sample_size: int = 1000,
    even_spacing: int = 1000,
) -> str:
    """Compute a format-independent fingerprint of tabular content.

    The fingerprint combines the (sorted) schema, the row count, and a
    hash over sampled rows with stringified values. It is stable across
    formats (dbc, parquet, csv, json) as long as the data and columns are
    equivalent, and it is computed only on parsed/decompressed content.

    Parameters
    ----------
    frame : pd.DataFrame
        The parsed/decompressed content.
    sample_size : int, optional
        Number of rows to sample from the head.
    even_spacing : int, optional
        Take one row every *even_spacing* rows to cover the whole file.

    Returns
    -------
    str
        Hex digest of the content fingerprint.
    """
    schema = tuple(
        sorted((str(col), str(dtype)) for col, dtype in frame.dtypes.items())
    )
    total_rows = len(frame)

    ordered = frame[sorted(frame.columns, key=str)]
    sampled = list(
        ordered.head(sample_size).astype(str).itertuples(index=False, name=None)
    )
    if total_rows > sample_size:
        sampled.extend(
            ordered.iloc[::even_spacing]
            .astype(str)
            .itertuples(index=False, name=None)
        )

    digest = hashlib.sha256()
    digest.update(repr(schema).encode())
    digest.update(str(total_rows).encode())
    for row in sampled:
        digest.update(repr(row).encode())
    return digest.hexdigest()


def byte_hash(
    path, algorithm: str = "sha256", chunk_size: int = 1024 * 1024
) -> str:
    """Compute the byte-level hash of a local file (exact identity only)."""
    hash_obj = hashlib.new(algorithm)
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()
