"""Data diff utilities for comparing DataFrames across versions.

Usage::

    from pysus.api.diff import diff_dfs, diff_summary, diff_rows

    diff = diff_dfs(df_old, df_new)
    summary = diff_summary(df_old, df_new)
    changed = diff_rows(df_old, df_new)
"""

from pysus.api.diff.comparison import diff_dfs, diff_rows, diff_summary

__all__ = ["diff_dfs", "diff_rows", "diff_summary"]
