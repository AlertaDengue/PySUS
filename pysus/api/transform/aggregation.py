"""Aggregation helpers for DATASUS DataFrames.

Provides state-level, age-group, and time-period aggregations.

Usage::

    from pysus.api.transform.aggregation import aggregate_by_state

    state_counts = aggregate_by_state(df, "DT_NOTIFIC")
"""

from __future__ import annotations

from typing import Literal

import pandas as pd


def aggregate_by_state(
    df: pd.DataFrame,
    value_col: str,
    agg_func: Literal["count", "sum", "mean", "median"] = "count",
) -> pd.DataFrame:
    """Aggregate data by state (UF).

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with UF column.
    value_col : str
        Column to aggregate.
    agg_func : str
        Aggregation function.

    Returns
    -------
    pd.DataFrame
        State-level aggregation.
    """
    uf_col = _detect_uf_column(df)
    if uf_col is None:
        raise ValueError("No UF column found in DataFrame")

    return df.groupby(uf_col)[value_col].agg(agg_func).reset_index()


def aggregate_by_age_group(
    df: pd.DataFrame,
    age_col: str,
    value_col: str,
    bins: list[int] | None = None,
) -> pd.DataFrame:
    """Aggregate data by age groups.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    age_col : str
        Age column name.
    value_col : str
        Column to aggregate.
    bins : list, optional
        Age group boundaries. Default: ``[0, 5, 15, 30, 50, 70, 120]``.

    Returns
    -------
    pd.DataFrame
        Age group aggregation.
    """
    if bins is None:
        bins = [0, 5, 15, 30, 50, 70, 120]

    labels = [f"{bins[i]}-{bins[i + 1]}" for i in range(len(bins) - 1)]
    df = df.copy()
    df["age_group"] = pd.cut(df[age_col], bins=bins, labels=labels, right=False)

    return (
        df.groupby("age_group", observed=False)[value_col].count().reset_index()
    )


def aggregate_by_period(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    freq: Literal["M", "Q", "Y"] = "M",
) -> pd.DataFrame:
    """Aggregate data by time period.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    date_col : str
        Date column (YYYYMMDD format).
    value_col : str
        Column to aggregate.
    freq : str
        Frequency (``"M"``=monthly, ``"Q"``=quarterly, ``"Y"``=yearly).

    Returns
    -------
    pd.DataFrame
        Period aggregation.
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df[date_col], format="%Y%m%d", errors="coerce")
    df["period"] = df["date"].dt.to_period(freq)

    return df.groupby("period")[value_col].count().reset_index()


def _detect_uf_column(df: pd.DataFrame) -> str | None:
    """Detect UF column name."""
    candidates = ["UF", "UF_RES", "SG_UF", "UF_ZI"]
    for col in candidates:
        if col in df.columns:
            return col
    return None
