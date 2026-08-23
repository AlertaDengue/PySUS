"""Public implementation layer for PySUS.

This module is the single entry-point that re-exports **every**
user-facing function, class, and constant into the ``pysus``
namespace.  ``pysus/__init__.py`` does ``from pysus.api._impl import *``
so that end-users only ever need::

    import pysus
    pysus.sinan("DENG", 2020, as_dataframe=True)

The ``__all__`` list below is the canonical inventory of what is
available at the top level.
"""

# ── Discovery & UI ──────────────────────────────────────────────
from pysus.api._impl._ui import _DADOSGOV_DESC, _FTP_DESC, info_table, search

# ── Dataset access (per-database convenience functions) ─────────
from pysus.api._impl.databases import (
    ciha,
    cnes,
    ibge,
    list_files,
    pni,
    sia,
    sih,
    sim,
    sinan,
    sinasc,
)

# ── Cache management ────────────────────────────────────────────
from pysus.api.cache_utils import cache_status, clear_cache

# ── Core orchestrator ───────────────────────────────────────────
from pysus.api.client import PySUS

# ── Column metadata & search ────────────────────────────────────
from pysus.api.columns import ColumnInfo, search_columns

# ── Parallel downloads ──────────────────────────────────────────
from pysus.api.concurrent import download_many

# ── Data diff ───────────────────────────────────────────────────
from pysus.api.diff import diff_dfs, diff_rows, diff_summary

# ── Errors & warnings ───────────────────────────────────────────
from pysus.api.errors import (
    AuthenticationError,
    CatalogError,
    ConnectionError,
    ConversionError,
    DownloadError,
    FormatError,
    ParseError,
    PySUSError,
    PySUSWarning,
    ValidationError,
    warn,
)

# ── Export helpers ──────────────────────────────────────────────
from pysus.api.export import export, to_csv, to_excel, to_geojson, to_sql

# ── JSON flattening ─────────────────────────────────────────────
from pysus.api.flatten import flatten_json_columns

# ── Portuguese → English mappings ───────────────────────────────
from pysus.api.mappings import to_english

# ── Schema metadata ─────────────────────────────────────────────
from pysus.api.metadata.columns import available_databases, load_column_metadata

# ── Retry & partial resume ──────────────────────────────────────
from pysus.api.partial import PartialDownload

# ── Progress controls ───────────────────────────────────────────
from pysus.api.progress import disable_progress_bars, enable_progress_bars

# ── Data quality ────────────────────────────────────────────────
from pysus.api.quality import (
    column_stats,
    missing_values,
    profile_report,
    quality_score,
    validate_data,
)
from pysus.api.retry import retry

# ── Streaming / DuckDB ─────────────────────────────────────────
from pysus.api.streaming import query_parquet, to_arrow, to_df

# ── Data transformation ─────────────────────────────────────────
from pysus.api.transform import (
    DetectedUnit,
    aggregate_by_age_group,
    aggregate_by_period,
    aggregate_by_state,
    detect_units,
    get_aliases,
    get_linking_keys,
    link_datasets,
    mask_data,
    optimize_memory,
    rename_columns,
    set_precision,
    stream_parquet,
    unmask_data,
)

# ── Input validation ────────────────────────────────────────────
from pysus.api.validate import (
    validate_choice,
    validate_dataset,
    validate_origin,
)

__all__ = [
    # ── Discovery & UI ──────────────────────────────────────────
    "info_table",
    "search",
    # ── Dataset access ──────────────────────────────────────────
    "cnes",
    "ciha",
    "ibge",
    "list_files",
    "pni",
    "sia",
    "sih",
    "sim",
    "sinan",
    "sinasc",
    # ── Cache management ────────────────────────────────────────
    "cache_status",
    "clear_cache",
    # ── Parallel downloads ──────────────────────────────────────
    "download_many",
    # ── Streaming / DuckDB ─────────────────────────────────────
    "query_parquet",
    "to_arrow",
    "to_df",
    # ── Export helpers ──────────────────────────────────────────
    "export",
    "to_csv",
    "to_excel",
    "to_geojson",
    "to_sql",
    # ── Data quality ────────────────────────────────────────────
    "column_stats",
    "missing_values",
    "profile_report",
    "quality_score",
    "validate_data",
    # ── Data transformation ─────────────────────────────────────
    "DetectedUnit",
    "aggregate_by_age_group",
    "aggregate_by_period",
    "aggregate_by_state",
    "detect_units",
    "get_aliases",
    "get_linking_keys",
    "link_datasets",
    "mask_data",
    "optimize_memory",
    "rename_columns",
    "set_precision",
    "stream_parquet",
    "unmask_data",
    # ── Data diff ───────────────────────────────────────────────
    "diff_dfs",
    "diff_rows",
    "diff_summary",
    # ── Column metadata & search ────────────────────────────────
    "ColumnInfo",
    "search_columns",
    # ── JSON flattening ─────────────────────────────────────────
    "flatten_json_columns",
    # ── Portuguese → English mappings ───────────────────────────
    "to_english",
    # ── Progress controls ───────────────────────────────────────
    "disable_progress_bars",
    "enable_progress_bars",
    # ── Schema metadata ─────────────────────────────────────────
    "available_databases",
    "load_column_metadata",
    # ── Errors & warnings ───────────────────────────────────────
    "AuthenticationError",
    "CatalogError",
    "ConnectionError",
    "ConversionError",
    "DownloadError",
    "FormatError",
    "ParseError",
    "PySUSError",
    "PySUSWarning",
    "ValidationError",
    "warn",
    # ── Core orchestrator ───────────────────────────────────────
    "PySUS",
    # ── Retry & partial resume ──────────────────────────────────
    "PartialDownload",
    "retry",
    # ── Input validation ────────────────────────────────────────
    "validate_choice",
    "validate_dataset",
    "validate_origin",
]

# Backward-compat aliases used by CLI modules
_DADOSGOV_DESCRIPTIONS = _DADOSGOV_DESC
_FTP_DESCRIPTIONS = _FTP_DESC
