"""Column metadata loader for DATASUS databases.

Provides bilingual column definitions from YAML schema files and
SINAN typecast dictionaries. Each column includes name, type,
description (PT/EN), format, and whether it's required.

Usage::

    from pysus.api.metadata.columns import load_column_metadata

    meta = load_column_metadata("sinan", group="arboviroses")
    # {'DT_NOTIFIC': {'type': 'string',
    #   'description': 'Data da notificação', ...}}
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

_SCHEMAS_DIR = Path(__file__).parent / "schemas"
_cache: dict[str, dict[str, Any]] = {}


def load_column_metadata(
    database: str,
    group: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Load column metadata for a DATASUS database.

    Parameters
    ----------
    database : str
        Database name: ``"sinan"``, ``"sih"``, ``"sia"``, ``"sim"``,
        ``"sinasc"``.
    group : str, optional
        Sub-group within the database (e.g. ``"arboviroses"`` for SINAN).
        When ``None``, returns metadata for all groups.

    Returns
    -------
    dict[str, dict]
        Mapping of column names (uppercase) to their definitions::

            {
                'DT_NOTIFIC': {
                    'type': 'string',
                    'description_pt': 'Data da notificação',
                    'description_en': 'Notification date',
                    'format': 'YYYYMMDD',
                    'required': True
                }
            }
    """
    database = database.lower().strip()
    cache_key = f"{database}:{group or ''}"

    if cache_key in _cache:
        return _cache[cache_key]

    result: dict[str, dict[str, Any]] = {}

    # Load from YAML schemas
    yaml_cols = _load_yaml_metadata(database, group)
    result.update(yaml_cols)

    # Load from SINAN typecast if available
    if database == "sinan":
        typecast_cols = _load_typecast_metadata()
        for col_name, col_meta in typecast_cols.items():
            if col_name not in result:
                result[col_name] = col_meta

    _cache[cache_key] = result
    return result


def _load_yaml_metadata(
    database: str,
    group: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Load column definitions from YAML schema files."""
    result: dict[str, dict[str, Any]] = {}

    # Check for database-specific schema directory
    db_dir = _SCHEMAS_DIR / database
    if db_dir.exists():
        for yaml_file in db_dir.glob("*.yaml"):
            if group and yaml_file.stem != group:
                continue
            with open(yaml_file, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
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
                            "format": col_def.get("format", ""),
                            "required": col_def.get("required", False),
                            "categories": col_def.get("categories", ""),
                            "characteristics": col_def.get(
                                "characteristics", ""
                            ),
                        }

    # Also check saude/schemas (existing location — SINAN disease groups)
    if database == "sinan":
        saude_dir = Path(__file__).parent.parent / "saude" / "schemas"
        if saude_dir.exists():
            for yaml_file in saude_dir.glob("*.yaml"):
                if group and yaml_file.stem != group:
                    continue
                with open(yaml_file, encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                for _ep_name, columns in data.items():
                    if not isinstance(columns, list):
                        continue
                    for col_def in columns:
                        col_name = col_def.get("name", "").upper()
                        if col_name and col_name not in result:
                            result[col_name] = {
                                "type": col_def.get("type", "string"),
                                "description_pt": col_def.get(
                                    "description_pt", ""
                                ),
                                "description_en": col_def.get(
                                    "description_en", ""
                                ),
                                "format": col_def.get("format", ""),
                                "required": col_def.get("required", False),
                                "categories": col_def.get("categories", ""),
                                "characteristics": col_def.get(
                                    "characteristics", ""
                                ),
                            }

    return result


def _load_typecast_metadata() -> dict[str, dict[str, Any]]:
    """Load column metadata from SINAN typecast dictionary."""
    try:
        from pysus.data.metadata.SINAN.typecast import COLUMN_TYPE
    except ImportError:
        return {}

    from pysus.api.mappings import PT_TO_EN

    result: dict[str, dict[str, Any]] = {}
    for col_name, sa_type in COLUMN_TYPE.items():
        en_name = PT_TO_EN.get(col_name, "")
        result[col_name] = {
            "type": str(sa_type).lower(),
            "description_pt": "",
            "description_en": en_name,
            "format": "",
            "required": False,
        }

    return result


def available_databases() -> list[str]:
    """Return list of databases with column metadata."""
    databases: set[str] = set()

    # Check schema directories
    if _SCHEMAS_DIR.exists():
        for db_dir in _SCHEMAS_DIR.iterdir():
            if db_dir.is_dir() and any(db_dir.glob("*.yaml")):
                databases.add(db_dir.name)

    # Check saude/schemas
    saude_dir = Path(__file__).parent.parent / "saude" / "schemas"
    if saude_dir.exists():
        for yaml_file in saude_dir.glob("*.yaml"):
            databases.add(yaml_file.stem)

    # SINAN always available (typecast)
    databases.add("sinan")

    return sorted(databases)


def available_groups(database: str) -> list[str]:
    """Return list of groups for a database."""
    groups: list[str] = []

    db_dir = _SCHEMAS_DIR / database
    if db_dir.exists():
        groups.extend(p.stem for p in db_dir.glob("*.yaml"))

    saude_dir = Path(__file__).parent.parent / "saude" / "schemas"
    if saude_dir.exists():
        for yaml_file in saude_dir.glob("*.yaml"):
            if yaml_file.stem not in groups:
                groups.append(yaml_file.stem)

    return sorted(groups)
