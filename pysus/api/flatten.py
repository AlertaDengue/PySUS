"""JSON column flattening for pandas DataFrames.

Detects columns containing JSON strings and normalizes them into flat
columns using ``pandas.json_normalize``.

Examples
--------
>>> from pysus.api.flatten import flatten_json_columns
>>> df = flatten_json_columns(df)
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def flatten_json_columns(
    df: pd.DataFrame,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Flatten columns containing JSON strings into flat columns.

    For each column specified (or auto-detected), parses JSON strings
    and expands them into new columns with a ``<original>.<key>``
    naming pattern.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    columns : list of str, optional
        Specific columns to flatten.  When ``None`` (default),
        auto-detects columns where a sample of non-null values
        looks like JSON objects.

    Returns
    -------
    DataFrame
        A copy with JSON columns expanded into flat columns and the
        original JSON columns dropped.

    Notes
    -----
    - Nested lists are kept as-is (not recursively flattened).
    - JSON objects with differing keys across rows are unioned; missing
      keys become ``NaN``.
    - Non-JSON values in a column are left as-is (column is not dropped).
    """
    import pandas as pd

    if df.empty:
        return df.copy()

    if columns is None:
        columns = _detect_json_columns(df)

    if not columns:
        return df.copy()

    result = df.copy()

    for col in columns:
        if col not in result.columns:
            continue

        series = result[col].dropna()
        if series.empty:
            continue

        parsed = series.map(_safe_parse_json)
        is_json = parsed.map(lambda x: isinstance(x, dict))

        if not is_json.any():
            continue

        flat = pd.json_normalize(parsed[is_json])
        flat.index = is_json[is_json].index

        result = result.drop(columns=[col])
        result = pd.concat([result, flat], axis=1)

    return result


def _detect_json_columns(df: pd.DataFrame) -> list[str]:
    """Auto-detect columns where values look like JSON objects."""
    detected: list[str] = []

    for col in df.select_dtypes(include=["object"]).columns:
        sample = df[col].dropna().head(20)
        if sample.empty:
            continue
        json_count = sum(1 for v in sample if _is_json_object(str(v)))
        if json_count >= len(sample) * 0.5:
            detected.append(col)

    return detected


def _is_json_object(s: str) -> bool:
    """Return True if *s* looks like a JSON object."""
    s = s.strip()
    if not s.startswith("{") or not s.endswith("}"):
        return False
    try:
        obj = json.loads(s)
        return isinstance(obj, dict)
    except (json.JSONDecodeError, TypeError):
        return False


def _safe_parse_json(s: str) -> dict | str:
    """Try to parse a string as JSON; return original on failure."""
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else s
    except (json.JSONDecodeError, TypeError):
        return s
