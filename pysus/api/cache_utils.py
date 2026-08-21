"""Smart cache utilities for PySUS.

Provides cache management: status, freshness check, and sync.

Usage::

    from pysus.api.cache_utils import cache_status, is_cache_fresh

    status = cache_status()
    print(status["total_size_mb"])
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("pysus.cache")


@dataclass
class CacheStatus:
    """Status of the PySUS cache directory."""

    path: Path
    total_files: int
    total_size_bytes: int
    parquet_files: int
    partial_files: int
    last_modified: datetime | None

    @property
    def total_size_mb(self) -> float:
        """Total cache size in megabytes."""
        return self.total_size_bytes / (1024 * 1024)


def cache_status(cache_path: Path | None = None) -> CacheStatus:
    """Get status of the cache directory.

    Parameters
    ----------
    cache_path : Path, optional
        Cache directory. If None, uses default.

    Returns
    -------
    CacheStatus
        Cache statistics.
    """
    if cache_path is None:
        from pysus import CACHEPATH

        cache_path = CACHEPATH

    if not cache_path.exists():
        return CacheStatus(
            path=cache_path,
            total_files=0,
            total_size_bytes=0,
            parquet_files=0,
            partial_files=0,
            last_modified=None,
        )

    total_files = 0
    total_size = 0
    parquet_count = 0
    partial_count = 0
    last_mod: datetime | None = None

    for f in cache_path.rglob("*"):
        if f.is_file():
            total_files += 1
            stat = f.stat()
            total_size += stat.st_size

            if f.suffix == ".parquet":
                parquet_count += 1
            if f.suffix == ".partial" or ".partial." in f.name:
                partial_count += 1

            mtime = datetime.fromtimestamp(stat.st_mtime)
            if last_mod is None or mtime > last_mod:
                last_mod = mtime

    return CacheStatus(
        path=cache_path,
        total_files=total_files,
        total_size_bytes=total_size,
        parquet_files=parquet_count,
        partial_files=partial_count,
        last_modified=last_mod,
    )


def is_cache_fresh(
    local_path: Path,
    remote_mtime: datetime | None = None,
) -> bool:
    """Check if a cached file is still fresh.

    Parameters
    ----------
    local_path : Path
        Path to the local cached file.
    remote_mtime : datetime, optional
        Last modification time of the remote file.

    Returns
    -------
    bool
        True if the local file is up to date.
    """
    if not local_path.exists():
        return False

    if remote_mtime is None:
        return True

    local_mtime = datetime.fromtimestamp(local_path.stat().st_mtime)
    return local_mtime >= remote_mtime


def clear_cache(cache_path: Path | None = None) -> int:
    """Clear all cached files.

    Parameters
    ----------
    cache_path : Path, optional
        Cache directory. If None, uses default.

    Returns
    -------
    int
        Number of files removed.
    """
    if cache_path is None:
        from pysus import CACHEPATH

        cache_path = CACHEPATH

    count = 0
    if cache_path.exists():
        for f in cache_path.rglob("*"):
            if f.is_file():
                f.unlink()
                count += 1
    return count


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable string.

    Parameters
    ----------
    size_bytes : int
        Size in bytes.

    Returns
    -------
    str
        Formatted string (e.g. '1.5 GB').
    """
    remaining = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if remaining < 1024:
            return f"{remaining:.1f} {unit}"
        remaining /= 1024
    return f"{remaining:.1f} PB"
