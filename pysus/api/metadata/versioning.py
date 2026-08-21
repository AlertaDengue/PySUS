"""Schema versioning for DATASUS databases.

Tracks which columns exist in each year's data layout and detects
changes between years (added/removed columns, type changes).

Usage::

    from pysus.api.metadata.versioning import (
        get_schema_version, list_schema_versions, detect_schema_change
    )

    version = get_schema_version("sinan", 2024)
    changes = detect_schema_change("sinan", 2023, 2024)
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from pysus.api.metadata.columns import load_column_metadata

_VERSIONS_FILE = Path(__file__).parent / "schema_versions.json"


def get_schema_version(database: str, year: int) -> str:
    """Get schema version identifier for a database/year.

    Parameters
    ----------
    database : str
        Database name (e.g. ``"sinan"``).
    year : int
        Year of the data.

    Returns
    -------
    str
        Version string like ``"sinan_v2024"`` or a hash fingerprint.
    """
    versions = _load_versions()
    db_versions = versions.get(database, {})

    if str(year) in db_versions:
        return db_versions[str(year)]

    # Auto-generate version from column metadata
    meta = load_column_metadata(database)
    if meta:
        cols_hash = hashlib.md5(
            json.dumps(sorted(meta.keys()), sort_keys=True).encode()
        ).hexdigest()[:8]
        return f"{database}_v{year}_{cols_hash}"

    return f"{database}_v{year}"


def list_schema_versions(database: str) -> dict[int, str]:
    """List all known schema versions for a database.

    Parameters
    ----------
    database : str
        Database name.

    Returns
    -------
    dict[int, str]
        Mapping of year → version string.
    """
    versions = _load_versions()
    db_versions = versions.get(database, {})
    return {int(y): v for y, v in db_versions.items()}


def detect_schema_change(
    database: str,
    year1: int,
    year2: int,
) -> dict[str, Any]:
    """Detect schema changes between two years.

    Compares column sets from the YAML metadata for both years.

    Parameters
    ----------
    database : str
        Database name.
    year1 : int
        Earlier year.
    year2 : int
        Later year.

    Returns
    -------
    dict
        ``{"added": [...], "removed": [...], "changed": {...},
        "version1": str, "version2": str}``
    """
    meta1 = _load_metadata_for_year(database, year1)
    meta2 = _load_metadata_for_year(database, year2)

    cols1 = set(meta1.keys())
    cols2 = set(meta2.keys())

    added = sorted(cols2 - cols1)
    removed = sorted(cols1 - cols2)

    # Detect type changes
    changed: dict[str, dict[str, str]] = {}
    for col in sorted(cols1 & cols2):
        t1 = meta1[col].get("type", "")
        t2 = meta2[col].get("type", "")
        if t1 != t2:
            changed[col] = {"old_type": t1, "new_type": t2}

    return {
        "added": added,
        "removed": removed,
        "changed": changed,
        "version1": get_schema_version(database, year1),
        "version2": get_schema_version(database, year2),
    }


def schema_fingerprint(database: str, year: int | None = None) -> str:
    """Compute a fingerprint of the schema for cache invalidation.

    Parameters
    ----------
    database : str
        Database name.
    year : int, optional
        Specific year. If None, uses current metadata.

    Returns
    -------
    str
        MD5 fingerprint of the sorted column names.
    """
    if year is not None:
        meta = _load_metadata_for_year(database, year)
    else:
        meta = load_column_metadata(database)

    cols = sorted(meta.keys())
    return hashlib.md5(json.dumps(cols, sort_keys=True).encode()).hexdigest()


def _load_versions() -> dict[str, dict[str, str]]:
    """Load version definitions from JSON file."""
    if not _VERSIONS_FILE.exists():
        return {}
    try:
        return json.loads(_VERSIONS_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _load_metadata_for_year(
    database: str, year: int
) -> dict[str, dict[str, Any]]:
    """Load metadata for a specific year.

    Currently returns the same metadata regardless of year (YAML files
    don't have year variants yet). This allows future extension where
    year-specific YAML files can be added.
    """
    # Check for year-specific schema file
    db_dir = Path(__file__).parent / "schemas" / database
    year_file = db_dir / f"{year}.yaml"
    if year_file.exists():
        import yaml  # type: ignore[import-untyped]

        with open(year_file) as f:
            data = yaml.safe_load(f) or {}
        result: dict[str, dict[str, Any]] = {}
        for _ep_name, columns in data.items():
            if not isinstance(columns, list):
                continue
            for col_def in columns:
                col_name = col_def.get("name", "").upper()
                if col_name:
                    result[col_name] = {
                        "type": col_def.get("type", "string"),
                        "description_pt": col_def.get("description_pt", ""),
                        "description_en": col_def.get("description_en", ""),
                    }
        return result

    return load_column_metadata(database)
