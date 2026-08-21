"""Local metadata cache with TTL support.

Caches metadata in ``~/.cache/pysus/metadata/`` with configurable
time-to-live. Cache keys are MD5 hashes of the query parameters.

Usage::

    from pysus.api.metadata.cache import (
        get_cached_metadata, set_cached_metadata
    )

    cached = get_cached_metadata("sinan:arboviroses")
    if cached is None:
        cached = expensive_metadata_lookup(...)
        set_cached_metadata("sinan:arboviroses", cached)
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

_CACHE_DIR = Path.home() / ".cache" / "pysus" / "metadata"
_DEFAULT_TTL = timedelta(days=7)


def get_cached_metadata(
    key: str,
    ttl: timedelta = _DEFAULT_TTL,
) -> dict[str, Any] | None:
    """Get cached metadata if valid.

    Parameters
    ----------
    key : str
        Cache key (e.g. ``"sinan:arboviroses"``).
    ttl : timedelta
        Time-to-live. Expired entries return None.

    Returns
    -------
    dict or None
        Cached metadata, or None if missing/expired.
    """
    cache_file = _cache_path(key)
    if not cache_file.exists():
        return None

    try:
        data = json.loads(cache_file.read_text(encoding="utf-8"))
        cached_at = datetime.fromisoformat(data["cached_at"])
        if datetime.now() - cached_at > ttl:
            return None
        return data["metadata"]
    except (json.JSONDecodeError, KeyError, ValueError):
        return None


def set_cached_metadata(
    key: str,
    metadata: dict[str, Any],
) -> Path:
    """Cache metadata with timestamp.

    Parameters
    ----------
    key : str
        Cache key.
    metadata : dict
        Metadata to cache (must be JSON-serializable).

    Returns
    -------
    Path
        Path to the cache file.
    """
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = _cache_path(key)

    data = {
        "cached_at": datetime.now().isoformat(),
        "metadata": metadata,
    }
    cache_file.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return cache_file


def invalidate_metadata(key: str) -> bool:
    """Remove a cached entry.

    Returns True if the entry existed and was removed.
    """
    cache_file = _cache_path(key)
    if cache_file.exists():
        cache_file.unlink()
        return True
    return False


def clear_cache() -> int:
    """Remove all cached metadata entries.

    Returns the number of files removed.
    """
    count = 0
    if _CACHE_DIR.exists():
        for cache_file in _CACHE_DIR.glob("*.json"):
            cache_file.unlink()
            count += 1
    return count


def cache_size() -> int:
    """Return number of cached entries."""
    if not _CACHE_DIR.exists():
        return 0
    return sum(1 for _ in _CACHE_DIR.glob("*.json"))


def _cache_path(key: str) -> Path:
    """Compute the file path for a cache key."""
    digest = hashlib.md5(key.encode()).hexdigest()
    return _CACHE_DIR / f"{digest}.json"
