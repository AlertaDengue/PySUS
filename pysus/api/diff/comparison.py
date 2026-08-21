"""DataFrame comparison engine for version diffs.

Usage::

    from pysus.api.diff.comparison import diff_dfs, diff_summary, diff_rows

    result = diff_dfs(df_old, df_new, key_cols=["UF", "MUNIC_RES"])
    summary = diff_summary(df_old, df_new)
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass
class DiffResult:
    """Result of a DataFrame comparison."""

    columns_added: list[str] = field(default_factory=list)
    columns_removed: list[str] = field(default_factory=list)
    columns_type_changed: list[tuple[str, str, str]] = field(
        default_factory=list
    )
    rows_added: int = 0
    rows_removed: int = 0
    rows_modified: int = 0
    rows_unchanged: int = 0
    modified_cells: list[tuple[str, int, str, str]] = field(
        default_factory=list
    )

    @property
    def summary(self) -> dict:
        """Summary dict of the diff."""
        return {
            "columns_added": len(self.columns_added),
            "columns_removed": len(self.columns_removed),
            "columns_type_changed": len(self.columns_type_changed),
            "rows_added": self.rows_added,
            "rows_removed": self.rows_removed,
            "rows_modified": self.rows_modified,
            "rows_unchanged": self.rows_unchanged,
        }


def diff_dfs(
    df_old: pd.DataFrame,
    df_new: pd.DataFrame,
    key_cols: list[str] | None = None,
) -> DiffResult:
    """Compare two DataFrames and return detailed diff.

    Parameters
    ----------
    df_old : pd.DataFrame
        Older/previous version.
    df_new : pd.DataFrame
        Newer/current version.
    key_cols : list, optional
        Columns identifying rows for matching. If None, compares by position.

    Returns
    -------
    DiffResult
        Detailed diff result.
    """
    result = DiffResult()

    # Column-level diffs
    old_cols = set(df_old.columns)
    new_cols = set(df_new.columns)
    result.columns_added = sorted(new_cols - old_cols)
    result.columns_removed = sorted(old_cols - new_cols)

    # Type changes
    for col in old_cols & new_cols:
        old_type = str(df_old[col].dtype)
        new_type = str(df_new[col].dtype)
        if old_type != new_type:
            result.columns_type_changed.append((col, old_type, new_type))

    # Row-level diffs
    if key_cols and all(c in old_cols for c in key_cols):
        result = _diff_by_key(df_old, df_new, key_cols, result)
    else:
        result = _diff_by_position(df_old, df_new, result)

    return result


def diff_summary(
    df_old: pd.DataFrame,
    df_new: pd.DataFrame,
) -> dict:
    """Quick summary diff without detailed cell-level comparison.

    Parameters
    ----------
    df_old : pd.DataFrame
        Older version.
    df_new : pd.DataFrame
        Newer version.

    Returns
    -------
    dict
        Summary of changes.
    """
    result = diff_dfs(df_old, df_new)
    return result.summary


def diff_rows(
    df_old: pd.DataFrame,
    df_new: pd.DataFrame,
    key_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Return rows that differ between two DataFrames.

    Parameters
    ----------
    df_old : pd.DataFrame
        Older version.
    df_new : pd.DataFrame
        Newer version.
    key_cols : list, optional
        Columns for matching rows.

    Returns
    -------
    pd.DataFrame
        DataFrame with diff info columns.
    """
    if key_cols and all(
        c in df_old.columns and c in df_new.columns for c in key_cols
    ):
        merged = df_new.merge(
            df_old,
            on=key_cols,
            how="outer",
            indicator=True,
            suffixes=("_new", "_old"),
        )
        return merged[merged["_merge"] != "both"]
    else:
        rows = []
        min_len = min(len(df_old), len(df_new))
        for i in range(min_len):
            if not df_old.iloc[i].equals(df_new.iloc[i]):
                rows.append(
                    {
                        "index": i,
                        "old": df_old.iloc[i].to_dict(),
                        "new": df_new.iloc[i].to_dict(),
                    }
                )
        return pd.DataFrame(rows)


def _diff_by_key(
    df_old: pd.DataFrame,
    df_new: pd.DataFrame,
    key_cols: list[str],
    result: DiffResult,
) -> DiffResult:
    """Diff by key columns (merge-based)."""
    merged = df_new.merge(
        df_old,
        on=key_cols,
        how="outer",
        indicator=True,
        suffixes=("_new", "_old"),
    )

    only_new = merged[merged["_merge"] == "left_only"]
    only_old = merged[merged["_merge"] == "right_only"]
    both = merged[merged["_merge"] == "both"]

    result.rows_added = len(only_new)
    result.rows_removed = len(only_old)

    # Count modified rows
    common_cols = [
        c for c in df_new.columns if c not in key_cols and c in df_old.columns
    ]

    for _, row in both.iterrows():
        for col in common_cols:
            new_val = row.get(f"{col}_new")
            old_val = row.get(f"{col}_old")
            if new_val != old_val:
                result.rows_modified += 1
                break
        else:
            result.rows_unchanged += 1

    return result


def _diff_by_position(
    df_old: pd.DataFrame,
    df_new: pd.DataFrame,
    result: DiffResult,
) -> DiffResult:
    """Diff by position (row-by-row)."""
    min_len = min(len(df_old), len(df_new))
    result.rows_added = abs(len(df_new) - len(df_old))
    if len(df_new) > len(df_old):
        result.rows_added = len(df_new) - len(df_old)
    else:
        result.rows_removed = len(df_old) - len(df_new)

    for i in range(min_len):
        if df_old.iloc[i].equals(df_new.iloc[i]):
            result.rows_unchanged += 1
        else:
            result.rows_modified += 1

    return result
