"""Column-level statistics for DataFrames.

Provides detailed per-column statistics including dtype, null counts,
unique values, and memory usage.

Usage::

    from pysus.api.quality.statistics import column_stats

    stats = column_stats(df)
    print(stats[["column", "null_pct", "memory_mb"]])
"""

from __future__ import annotations

import pandas as pd


def column_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Get detailed statistics for each column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: ``column``, ``dtype``, ``null_count``,
        ``null_pct``, ``unique_count``, ``unique_pct``,
        ``memory_bytes``, ``memory_mb``, ``sample_value``.
        Sorted by memory usage (descending).
    """
    results = []

    for col in df.columns:
        series = df[col]
        memory = int(series.memory_usage(deep=True))
        total = len(series)
        null_count = int(series.isna().sum())
        null_pct = null_count / total if total > 0 else 0.0
        unique_count = int(series.nunique())
        unique_pct = unique_count / total if total > 0 else 0.0

        sample = series.dropna().iloc[0] if not series.dropna().empty else None

        results.append(
            {
                "column": col,
                "dtype": str(series.dtype),
                "null_count": null_count,
                "null_pct": round(null_pct, 4),
                "unique_count": unique_count,
                "unique_pct": round(unique_pct, 4),
                "memory_bytes": memory,
                "memory_mb": round(memory / (1024 * 1024), 4),
                "sample_value": str(sample) if sample is not None else None,
            }
        )

    result_df = pd.DataFrame(results)
    if len(result_df) > 0:
        return result_df.sort_values(
            "memory_bytes", ascending=False
        ).reset_index(drop=True)
    return result_df
