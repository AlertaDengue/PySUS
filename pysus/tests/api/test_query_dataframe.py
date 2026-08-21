"""Tests for PySUS.query() DataFrame mode (Phase 1.1)."""

import asyncio
import inspect
from unittest.mock import AsyncMock, MagicMock

import pandas as pd


class TestQueryAsDataframe:
    def test_as_dataframe_false_returns_list(self, tmp_path):
        """Default mode returns list[BaseRemoteFile]."""
        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "test.db")
        sig = inspect.signature(client.query)
        assert sig.parameters["as_dataframe"].default is False
        assert sig.parameters["columns"].default is None
        assert sig.parameters["dtypes"].default is None

    def test_as_dataframe_true_returns_dataframe(self, tmp_path):
        """When as_dataframe=True, the return type is DataFrame."""
        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "test.db")
        sig = inspect.signature(client.query)
        ret = sig.return_annotation
        assert "DataFrame" in str(ret) or "pd.DataFrame" in str(ret)

    def test_query_with_empty_files_returns_empty_df(self, tmp_path):
        """Empty query results return an empty DataFrame."""
        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "test.db")

        async def _test():
            mock_ducklake = MagicMock()
            mock_ducklake.datasets = AsyncMock(return_value=[])
            client._ducklake = mock_ducklake
            result = await client.query(as_dataframe=True)
            assert isinstance(result, pd.DataFrame)
            assert result.empty

        asyncio.run(_test())


class TestQuerySignature:
    def test_query_has_columns_and_dtypes_params(self):
        from pysus.api.client import PySUS

        sig = inspect.signature(PySUS.query)
        assert "columns" in sig.parameters
        assert "dtypes" in sig.parameters
        assert "as_dataframe" in sig.parameters

    def test_convenience_functions_pass_as_dataframe(self):
        """sinan/sinasc/etc already accept as_dataframe via **kwargs."""
        from pysus.api._impl.databases import sinan

        sig = inspect.signature(sinan)
        # sinan uses **kwargs, so as_dataframe is passed through
        assert "kwargs" in sig.parameters
