"""Searchable column metadata across SUS health datasets.

Provides a unified interface to look up column names, types, and
bilingual descriptions across all available schema sources.

Examples
--------
>>> from pysus.api.columns import search_columns, ColumnInfo
>>> results = search_columns("dengue", "notification")
>>> for col in results:
...     print(col.name, col.description)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pysus.api.mappings import PT_TO_EN


@dataclass(frozen=True, slots=True)
class ColumnInfo:
    """Metadata for a single column in a SUS dataset.

    Attributes
    ----------
    name : str
        Column name as it appears in the data file (lowercase).
    description : str
        Human-readable description (Portuguese).
    description_en : str
        English description, or empty string if unavailable.
    dtype : str
        Data type string (e.g. ``"integer"``, ``"string"``,
        ``"VARCHAR(1)"``).
    dataset : str
        Parent dataset name (e.g. ``"arboviroses"``).
    endpoint : str
        Sub-grouping within the dataset (e.g. ``"dengue"``).
    """

    name: str
    description: str = ""
    description_en: str = ""
    dtype: str = ""
    dataset: str = ""
    endpoint: str = ""


def search_columns(
    dataset: str | None = None,
    query: str | None = None,
    *,
    endpoint: str | None = None,
) -> list[ColumnInfo]:
    """Search column metadata across all available schema sources.

    Parameters
    ----------
    dataset : str, optional
        Restrict to a specific dataset name (e.g. ``"arboviroses"``).
        When ``None``, searches all datasets.
    query : str, optional
        Case-insensitive substring match against column name and
        descriptions (both PT and EN).  When ``None``, returns all
        columns for the given dataset.
    endpoint : str, optional
        Restrict to a specific endpoint within the dataset.

    Returns
    -------
    list[ColumnInfo]
        Matching column metadata, sorted by name.

    Examples
    --------
    >>> search_columns("arboviroses", "date")
    [ColumnInfo(name='dt_notific', description='Data da notificação', ...)]
    """
    results: list[ColumnInfo] = []

    # Source 1: YAML schemas
    results.extend(_search_yaml_schemas(dataset, query, endpoint))

    # Source 2: SINAN typecast (types only)
    results.extend(_search_typecast(dataset, query))

    # Deduplicate by (dataset, name), keeping the entry with most info
    seen: dict[tuple[str, str], ColumnInfo] = {}
    for col in results:
        key = (col.dataset, col.name)
        existing = seen.get(key)
        if existing is None or len(col.description) > len(existing.description):
            seen[key] = col

    output = sorted(seen.values(), key=lambda c: c.name)
    if query:
        q = query.lower()
        output = [
            c
            for c in output
            if q in c.name.lower()
            or q in c.description.lower()
            or q in c.description_en.lower()
        ]

    return output


def _search_yaml_schemas(
    dataset: str | None,
    query: str | None,
    endpoint_filter: str | None,
) -> list[ColumnInfo]:
    """Search YAML schema files for column metadata."""
    from pysus.api.saude.schemas import available_schemas

    schemas = available_schemas()
    if dataset:
        schemas = [s for s in schemas if s == dataset]

    results: list[ColumnInfo] = []
    for ds_name in schemas:
        data = _load_yaml_raw(ds_name)
        for ep_name, cols in data.items():
            if endpoint_filter and ep_name != endpoint_filter:
                continue
            for col_def in cols:
                results.append(
                    ColumnInfo(
                        name=col_def["name"].lower(),
                        description=col_def.get("description_pt", ""),
                        description_en=col_def.get("description_en", ""),
                        dtype=col_def.get("type", ""),
                        dataset=ds_name,
                        endpoint=ep_name,
                    )
                )

    return results


def _search_typecast(
    dataset: str | None,
    query: str | None,
) -> list[ColumnInfo]:
    """Search the SINAN typecast dict for column types."""
    if dataset and dataset.upper() != "SINAN":
        return []

    try:
        from pysus.data.metadata.SINAN.typecast import COLUMN_TYPE
    except ImportError:
        return []

    results: list[ColumnInfo] = []
    for col_name, sa_type in COLUMN_TYPE.items():
        en_name = PT_TO_EN.get(col_name, "")
        dtype_str = str(sa_type)
        results.append(
            ColumnInfo(
                name=col_name.lower(),
                description="",
                description_en=en_name,
                dtype=dtype_str,
                dataset="sinan",
                endpoint="",
            )
        )

    return results


def _load_yaml_raw(name: str) -> dict[str, list[dict[str, Any]]]:
    """Load raw YAML data without the public API caching."""
    from pysus.api.saude.schemas import _load_yaml

    return _load_yaml(name)
