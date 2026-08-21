"""Missing value analysis for DataFrames.

Provides per-column and per-state missing value percentages with
configurable thresholds.

Usage::

    from pysus.api.quality.missing import missing_values

    result = missing_values(df, threshold=0.1)
    grouped = missing_values(df, group_by="UF")
"""

from __future__ import annotations

import pandas as pd


def missing_values(
    df: pd.DataFrame,
    group_by: str | None = None,
    threshold: float = 0.0,
) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
    """Analyze missing values in a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    group_by : str, optional
        Column to group by (e.g. ``"UF"`` for state-level analysis).
    threshold : float
        Minimum missing percentage to include (0.0-1.0).

    Returns
    -------
    pd.DataFrame or tuple
        DataFrame with columns: ``column``, ``missing_count``,
        ``missing_pct``, ``complete_count``, ``complete_pct``.
        If ``group_by`` is provided, returns a tuple of
        ``(summary_df, grouped_df)``.
    """
    results = []

    for col in df.columns:
        null_count = int(df[col].isna().sum())
        total = len(df)
        missing_pct = null_count / total if total > 0 else 0.0

        if missing_pct >= threshold:
            results.append(
                {
                    "column": col,
                    "missing_count": null_count,
                    "missing_pct": round(missing_pct, 4),
                    "complete_count": total - null_count,
                    "complete_pct": round(1 - missing_pct, 4),
                }
            )

    result_df = pd.DataFrame(results)
    if len(result_df) > 0:
        result_df = result_df.sort_values("missing_pct", ascending=False)
        result_df = result_df.reset_index(drop=True)

    if group_by and group_by in df.columns:
        other_cols = [c for c in df.columns if c != group_by]
        grouped = (
            df.groupby(group_by)[other_cols]
            .apply(lambda x: x.isna().mean())
            .reset_index()
        )
        return result_df, grouped

    return result_df
