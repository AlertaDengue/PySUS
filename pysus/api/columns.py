"""Searchable column metadata across SUS health datasets.

Provides a unified interface to look up column names, types, bilingual
descriptions, value categories and field characteristics across all
available schema sources (saude endpoints and SINAN disease forms).

Examples
--------
>>> from pysus.api.columns import search_columns, ColumnInfo
>>> results = search_columns("dengue", "notification")
>>> for col in results:
...     print(col.name, col.description)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from pysus.api.mappings import PT_TO_EN

_SINAN_SCHEMAS_DIR = Path(__file__).parent / "metadata" / "schemas" / "sinan"


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
        Parent dataset name (e.g. ``"arboviroses"``, ``"sinan"``).
    endpoint : str
        Sub-grouping within the dataset (e.g. ``"dengue"``, or the
        SINAN disease form such as ``"peste"``).
    categories : str
        Value codes of the field (e.g. ``"1-Sim 2-Não 9-Ignorado"``),
        or empty when unavailable.
    characteristics : str
        Field rules and notes from the official dictionary (obligatory,
        dependencies, validation), or empty when unavailable.
    required : bool
        Whether the field is mandatory on the notification form.
    format : str
        Expected format hint (e.g. ``"YYYYMMDD"``), if any.
    """

    name: str
    description: str = ""
    description_en: str = ""
    dtype: str = ""
    dataset: str = ""
    endpoint: str = ""
    categories: str = ""
    characteristics: str = ""
    required: bool = False
    format: str = ""


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
        Restrict to a specific dataset name (e.g. ``"arboviroses"`` or
        ``"sinan"``). When ``None``, searches all datasets.
    query : str, optional
        Case-insensitive substring match against column name and
        descriptions (both PT and EN).  When ``None``, returns all
        columns for the given dataset.
    endpoint : str, optional
        Restrict to a specific endpoint within the dataset (e.g.
        ``"dengue"``, or a SINAN disease form such as ``"peste"``).

    Returns
    -------
    list[ColumnInfo]
        Matching column metadata, sorted by name.

    Examples
    --------
    >>> search_columns("arboviroses", "date")
    [ColumnInfo(name='dt_notific', description='Data da notificação', ...)]
    >>> search_columns("sinan", endpoint="peste")
    [ColumnInfo(name='con_classi', description='', ...)]
    """
    results: list[ColumnInfo] = []

    # Source 1: YAML schemas (saude endpoints + SINAN disease forms)
    results.extend(_search_yaml_schemas(dataset, query, endpoint))

    # Source 2: SINAN typecast (types only)
    results.extend(_search_typecast(dataset, query))

    # Deduplicate by (dataset, name), merging fields so no source is lost
    seen: dict[tuple[str, str], ColumnInfo] = {}
    for col in results:
        key = (col.dataset, col.name)
        if key in seen:
            seen[key] = _merge_columns(seen[key], col)
        else:
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
            or q in c.categories.lower()
            or q in c.characteristics.lower()
        ]

    return output


def _merge_columns(a: ColumnInfo, b: ColumnInfo) -> ColumnInfo:
    """Merge two entries for the same column, keeping any available field."""
    return ColumnInfo(
        name=a.name,
        description=a.description or b.description,
        description_en=a.description_en or b.description_en,
        dtype=a.dtype or b.dtype,
        dataset=a.dataset,
        endpoint=a.endpoint or b.endpoint,
        categories=a.categories or b.categories,
        characteristics=a.characteristics or b.characteristics,
        required=a.required or b.required,
        format=a.format or b.format,
    )


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
                results.append(_column_info(col_def, ds_name, ep_name))

    # SINAN disease forms (metadata/schemas/sinan/*.yaml)
    if dataset in (None, "sinan"):
        for yaml_file in sorted(_SINAN_SCHEMAS_DIR.glob("*.yaml")):
            if endpoint_filter and yaml_file.stem != endpoint_filter:
                continue
            with open(yaml_file, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            for _ep_name, cols in data.items():
                if not isinstance(cols, list):
                    continue
                for col_def in cols:
                    if not isinstance(col_def, dict) or "name" not in col_def:
                        continue
                    results.append(
                        _column_info(col_def, "sinan", yaml_file.stem)
                    )

    return results


def _column_info(
    col_def: dict[str, Any], dataset: str, endpoint: str
) -> ColumnInfo:
    """Build a ColumnInfo from a YAML column definition dict."""
    return ColumnInfo(
        name=str(col_def["name"]).lower(),
        description=str(col_def.get("description_pt", "")),
        description_en=str(col_def.get("description_en", "")),
        dtype=str(col_def.get("type", "")),
        dataset=dataset,
        endpoint=endpoint,
        categories=str(col_def.get("categories", "")),
        characteristics=str(col_def.get("characteristics", "")),
        required=bool(col_def.get("required", False)),
        format=str(col_def.get("format", "")),
    )


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
