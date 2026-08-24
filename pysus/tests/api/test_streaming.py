"""Tests for pysus.api.streaming — DuckDB streaming DataFrames."""

import pandas as pd
import pytest
from pysus.api.streaming import query_parquet, to_arrow, to_df


@pytest.fixture
def sample_parquet(tmp_path):
    """Create a sample Parquet file."""
    df = pd.DataFrame(
        {
            "UF": ["RJ", "SP", "RJ", "SP"],
            "IDADE": [25, 30, 45, 60],
            "VALOR": [100.0, 200.0, 300.0, 400.0],
        }
    )
    path = tmp_path / "test.parquet"
    df.to_parquet(path)
    return path


class TestQueryParquet:
    def test_basic_query(self, sample_parquet):
        rel = query_parquet(sample_parquet)
        result = rel.to_df()
        assert len(result) == 4
        assert list(result.columns) == ["UF", "IDADE", "VALOR"]

    def test_custom_query(self, sample_parquet):
        rel = query_parquet(
            sample_parquet,
            "SELECT UF, COUNT(*) as cnt FROM data GROUP BY UF",
        )
        result = rel.to_df()
        assert len(result) == 2
        assert "cnt" in result.columns

    def test_with_glob(self, tmp_path):
        df = pd.DataFrame({"A": [1, 2]})
        df.to_parquet(tmp_path / "a.parquet")
        df.to_parquet(tmp_path / "b.parquet")

        rel = query_parquet(tmp_path / "*.parquet")
        result = rel.to_df()
        assert len(result) == 4

    def test_with_dir(self, tmp_path):
        df = pd.DataFrame({"A": [1, 2]})
        df.to_parquet(tmp_path / "data.parquet")

        rel = query_parquet(tmp_path)
        result = rel.to_df()
        assert len(result) == 2


class TestToDf:
    def test_materializes(self, sample_parquet):
        rel = query_parquet(sample_parquet)
        df = to_df(rel)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4


class TestToArrow:
    def test_exports(self, sample_parquet):
        import pyarrow as pa

        rel = query_parquet(sample_parquet)
        table = to_arrow(rel)
        assert isinstance(table, pa.Table)
        assert len(table) == 4
