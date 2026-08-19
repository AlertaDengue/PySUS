"""YAML schema loader for Saude DEMAS endpoint columns.

Provides optional manual column descriptions that override the
auto-inferred types from Parquet conversion. The YAML files live in
``pysus/api/saude/schemas/<dataset>.yaml`` and map endpoint names to
lists of column definitions.

Usage::

    from pysus.api.saude.schemas import load_endpoint_columns

    cols = load_endpoint_columns("arboviroses", "dengue")
    # [{"name": "nu_ano", "type": "integer",
    #   "description_pt": "Ano da notificação", ...}, ...]

The loader is cached after the first read.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

_SCHEMAS_DIR = Path(__file__).parent / "schemas"
_cache: dict[str, dict[str, list[dict[str, Any]]]] = {}


def _load_yaml(name: str) -> dict[str, list[dict[str, Any]]]:
    if name in _cache:
        return _cache[name]

    path = _SCHEMAS_DIR / f"{name}.yaml"
    if not path.exists():
        _cache[name] = {}
        return {}

    with open(path) as f:
        data = yaml.safe_load(f) or {}

    _cache[name] = data
    return data


def load_endpoint_columns(dataset: str, endpoint: str) -> list[dict[str, Any]]:
    """Return column definitions for *endpoint* within *dataset*.

    Parameters
    ----------
    dataset : str
        Dataset name (e.g. ``"arboviroses"``).
    endpoint : str
        Endpoint name (e.g. ``"dengue"``).

    Returns
    -------
    list[dict]
        Each dict has keys ``name``, ``type``, ``description_pt``,
        ``description_en``. Returns an empty list when no schema file
        exists or the endpoint is not found.
    """
    data = _load_yaml(dataset)
    return data.get(endpoint, [])


def available_schemas() -> list[str]:
    """Return names of datasets that have YAML schema files."""
    if not _SCHEMAS_DIR.exists():
        return []
    return sorted(p.stem for p in _SCHEMAS_DIR.glob("*.yaml"))


def apply_column_descriptions(
    columns_cursor: Any,
    dataset_id: int,
    dataset: str,
    endpoint: str,
) -> int:
    """Update column descriptions from YAML for *endpoint*.

    Returns the number of columns updated.
    """
    cols = load_endpoint_columns(dataset, endpoint)
    if not cols:
        return 0

    updated = 0
    for col in cols:
        columns_cursor.execute(
            "UPDATE pysus.dataset_columns "
            "SET description = ?, type = ? "
            "WHERE dataset_id = ? AND name = ?",
            (
                col.get("description_pt", ""),
                col.get("type", "VARCHAR"),
                dataset_id,
                col["name"],
            ),
        )
        updated += 1

    return updated
