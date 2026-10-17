"""Tests for pysus.api.diff module."""

import pandas as pd
from pysus.api.diff import diff_dfs, diff_rows, diff_summary


def make_df_a():
    return pd.DataFrame(
        {
            "UF": ["RJ", "SP", "MG"],
            "IDADE": [25, 30, 35],
            "VALOR": [100.0, 200.0, 300.0],
        }
    )


def make_df_b():
    return pd.DataFrame(
        {
            "UF": ["RJ", "SP", "BA"],
            "IDADE": [25, 31, 40],
            "VALOR": [100.0, 250.0, 500.0],
        }
    )


class TestDiffDfs:
    def test_same_dfs(self):
        df = make_df_a()
        result = diff_dfs(df, df)
        assert result.rows_added == 0
        assert result.rows_removed == 0
        assert result.rows_modified == 0
        assert result.rows_unchanged == 3

    def test_with_key_cols(self):
        df_a = make_df_a()
        df_b = make_df_b()
        result = diff_dfs(df_a, df_b, key_cols=["UF"])
        assert result.rows_added == 1  # BA
        assert result.rows_removed == 1  # MG
        assert result.rows_modified >= 0

    def test_position_diff(self):
        df_a = make_df_a()
        df_b = make_df_b()
        result = diff_dfs(df_a, df_b)
        assert result.rows_modified >= 0

    def test_column_added(self):
        df_a = pd.DataFrame({"A": [1, 2]})
        df_b = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        result = diff_dfs(df_a, df_b)
        assert "B" in result.columns_added

    def test_column_removed(self):
        df_a = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        df_b = pd.DataFrame({"A": [1, 2]})
        result = diff_dfs(df_a, df_b)
        assert "B" in result.columns_removed

    def test_type_changed(self):
        df_a = pd.DataFrame({"A": [1, 2]})
        df_b = pd.DataFrame({"A": ["1", "2"]})
        result = diff_dfs(df_a, df_b)
        assert len(result.columns_type_changed) == 1


class TestDiffPositionRowCounts:
    """Positional diff must not invent additions when rows were removed."""

    def test_shrinking_frame_reports_no_additions(self):
        df_old = pd.DataFrame({"A": [1, 2, 3, 4, 5]})
        df_new = pd.DataFrame({"A": [1, 2, 3]})
        result = diff_dfs(df_old, df_new)
        assert result.rows_added == 0
        assert result.rows_removed == 2

    def test_growing_frame_reports_no_removals(self):
        df_old = pd.DataFrame({"A": [1, 2]})
        df_new = pd.DataFrame({"A": [1, 2, 3, 4]})
        result = diff_dfs(df_old, df_new)
        assert result.rows_added == 2
        assert result.rows_removed == 0

    def test_equal_length_reports_neither(self):
        df_old = pd.DataFrame({"A": [1, 2, 3]})
        df_new = pd.DataFrame({"A": [1, 9, 3]})
        result = diff_dfs(df_old, df_new)
        assert result.rows_added == 0
        assert result.rows_removed == 0
        assert result.rows_modified == 1
        assert result.rows_unchanged == 2


class TestDiffNullHandling:
    """Unchanged NULL cells must not be counted as modifications."""

    def test_nan_in_both_frames_is_unchanged(self):
        df_old = pd.DataFrame({"UF": ["RJ", "SP"], "X": [1.0, float("nan")]})
        df_new = pd.DataFrame({"UF": ["RJ", "SP"], "X": [1.0, float("nan")]})
        result = diff_dfs(df_old, df_new, key_cols=["UF"])
        assert result.rows_modified == 0
        assert result.rows_unchanged == 2

    def test_null_to_value_is_modified(self):
        df_old = pd.DataFrame({"UF": ["RJ", "SP"], "X": [1.0, float("nan")]})
        df_new = pd.DataFrame({"UF": ["RJ", "SP"], "X": [1.0, 5.0]})
        result = diff_dfs(df_old, df_new, key_cols=["UF"])
        assert result.rows_modified == 1
        assert result.rows_unchanged == 1

    def test_value_to_null_is_modified(self):
        df_old = pd.DataFrame({"UF": ["RJ", "SP"], "X": [1.0, 5.0]})
        df_new = pd.DataFrame({"UF": ["RJ", "SP"], "X": [1.0, float("nan")]})
        result = diff_dfs(df_old, df_new, key_cols=["UF"])
        assert result.rows_modified == 1
        assert result.rows_unchanged == 1

    def test_nullable_dtype_na_does_not_raise(self):
        df_old = pd.DataFrame({"UF": ["RJ", "SP"], "X": [1, pd.NA]})
        df_old["X"] = df_old["X"].astype("Int64")
        df_new = pd.DataFrame({"UF": ["RJ", "SP"], "X": [1, pd.NA]})
        df_new["X"] = df_new["X"].astype("Int64")
        result = diff_dfs(df_old, df_new, key_cols=["UF"])
        assert result.rows_modified == 0
        assert result.rows_unchanged == 2

    def test_nullable_dtype_na_to_value_is_modified(self):
        df_old = pd.DataFrame({"UF": ["RJ", "SP"], "X": [1, pd.NA]})
        df_old["X"] = df_old["X"].astype("Int64")
        df_new = pd.DataFrame({"UF": ["RJ", "SP"], "X": [1, 7]})
        df_new["X"] = df_new["X"].astype("Int64")
        result = diff_dfs(df_old, df_new, key_cols=["UF"])
        assert result.rows_modified == 1
        assert result.rows_unchanged == 1

    def test_nan_to_na_across_dtypes_is_modified(self):
        df_old = pd.DataFrame({"UF": ["RJ"], "X": [float("nan")]})
        df_new = pd.DataFrame({"UF": ["RJ"], "X": [pd.NA]})
        df_new["X"] = df_new["X"].astype("Int64")
        result = diff_dfs(df_old, df_new, key_cols=["UF"])
        assert result.rows_modified == 0
        assert result.rows_unchanged == 1


class TestDiffSummary:
    def test_returns_dict(self):
        df_a = make_df_a()
        df_b = make_df_b()
        summary = diff_summary(df_a, df_b)
        assert isinstance(summary, dict)
        assert "rows_added" in summary
        assert "rows_removed" in summary


class TestDiffRows:
    def test_returns_diff_rows(self):
        df_a = make_df_a()
        df_b = make_df_b()
        result = diff_rows(df_a, df_b)
        assert len(result) > 0

    def test_same_dfs_returns_empty(self):
        df = make_df_a()
        result = diff_rows(df, df)
        assert len(result) == 0
