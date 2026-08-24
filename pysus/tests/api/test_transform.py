"""Tests for pysus.api.transform module."""

import numpy as np
import pandas as pd
import pytest
from pysus.api.transform import (
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


def make_test_df():
    """Create a test DataFrame."""
    return pd.DataFrame(
        {
            "UF": ["RJ", "SP", "RJ", "SP"],
            "IDADE": [25, 30, 45, 60],
            "DT_NOTIFIC": ["20240101", "20240115", "20240201", "20240215"],
            "PESO": [70.5, 85.0, 65.0, 90.0],
            "CS_SEXO": ["M", "F", "M", "F"],
            "VALOR": [100.0, 200.0, 300.0, 400.0],
        }
    )


class TestDetectUnits:
    def test_from_column_name(self):
        df = pd.DataFrame({"PESO": [70.0, 80.0]})
        units = detect_units(df)
        assert any(u.column == "PESO" and u.unit == "kg" for u in units)

    def test_from_metadata(self):
        df = pd.DataFrame({"TEMP": [36.5, 37.0]})
        metadata = {"TEMP": {"unit": "°C"}}
        units = detect_units(df, metadata=metadata)
        assert any(u.unit == "°C" and u.confidence == 1.0 for u in units)

    def test_empty_df(self):
        df = pd.DataFrame()
        units = detect_units(df)
        assert units == []

    def test_non_numeric_skipped(self):
        df = pd.DataFrame({"NOME": ["João", "Maria"]})
        units = detect_units(df)
        assert units == []


class TestLinkDatasets:
    def test_basic_link(self):
        df1 = pd.DataFrame({"MUNIC_RES": ["330045", "330050"], "VAL": [1, 2]})
        df2 = pd.DataFrame({"MUNIC_RES": ["330045", "330060"], "VAL": [3, 4]})
        result = link_datasets(df1, df2, on="MUNIC_RES")
        assert len(result) == 1  # inner join

    def test_linking_keys(self):
        keys = get_linking_keys("sinan", "sih")
        assert "MUNIC_RES" in keys

    def test_no_common_keys(self):
        keys = get_linking_keys("sinan", "ibge")
        assert keys == []


class TestAggregation:
    def test_by_state(self):
        df = make_test_df()
        result = aggregate_by_state(df, "VALOR", "count")
        assert "UF" in result.columns
        assert len(result) == 2  # RJ and SP

    def test_by_age_group(self):
        df = make_test_df()
        result = aggregate_by_age_group(df, "IDADE", "VALOR")
        assert "age_group" in result.columns

    def test_by_period(self):
        df = make_test_df()
        result = aggregate_by_period(df, "DT_NOTIFIC", "VALOR", freq="M")
        assert "period" in result.columns

    def test_no_uf_column(self):
        df = pd.DataFrame({"A": [1, 2]})
        try:
            aggregate_by_state(df, "A")
            raise AssertionError("Should have raised ValueError")
        except ValueError:
            pass


class TestStreaming:
    def test_stream_parquet(self, tmp_path):
        df = make_test_df()
        path = tmp_path / "test.parquet"
        df.to_parquet(path)

        chunks = list(stream_parquet(path, chunk_size=2))
        assert len(chunks) == 2
        result = pd.concat(chunks, ignore_index=True)
        assert len(result) == len(df)

    def test_stream_with_columns(self, tmp_path):
        df = make_test_df()
        path = tmp_path / "test.parquet"
        df.to_parquet(path)

        chunks = list(stream_parquet(path, columns=["UF", "IDADE"]))
        assert all(c.columns.tolist() == ["UF", "IDADE"] for c in chunks)


class TestAliases:
    def test_get_aliases(self):
        aliases = get_aliases("sinan", "DT_NOTIFIC")
        assert "DT_NOT" in aliases

    def test_get_aliases_unknown(self):
        aliases = get_aliases("sinan", "UNKNOWN_COL")
        assert aliases == []

    def test_rename_columns(self):
        df = pd.DataFrame({"DT_NOT": ["20240101"], "SEXO": ["M"]})
        result = rename_columns(df, database="sinan")
        assert "DT_NOTIFIC" in result.columns
        assert "CS_SEXO" in result.columns


try:
    import cryptography  # noqa: F401

    _has_cryptography = True
except ImportError:
    _has_cryptography = False


@pytest.mark.skipif(not _has_cryptography, reason="cryptography not installed")
class TestMasking:
    def test_encrypt_decrypt(self):
        df = pd.DataFrame({"CPF": ["12345678901", "98765432100"]})
        masked, key = mask_data(df, columns=["CPF"], method="encrypt")
        assert masked["CPF"].iloc[0] != "12345678901"

        unmasked = unmask_data(masked, ["CPF"], key)
        assert unmasked["CPF"].iloc[0] == "12345678901"

    def test_hash(self):
        df = pd.DataFrame({"CPF": ["12345678901"]})
        masked, _ = mask_data(df, columns=["CPF"], method="hash")
        assert masked["CPF"].iloc[0] != "12345678901"
        assert len(masked["CPF"].iloc[0]) == 64  # SHA256 hex

    def test_redact(self):
        df = pd.DataFrame({"CPF": ["12345678901"]})
        masked, _ = mask_data(df, columns=["CPF"], method="redact")
        assert masked["CPF"].iloc[0] == "***"

    def test_auto_detect_columns(self):
        df = pd.DataFrame({"CPF": ["123"], "NOME": ["João"]})
        masked, _ = mask_data(df, method="redact")
        assert masked["CPF"].iloc[0] == "***"
        assert masked["NOME"].iloc[0] == "***"


class TestPrecision:
    def test_set_float32(self):
        df = pd.DataFrame({"VAL": [1.0, 2.0, 3.0]})
        result = set_precision(df, precision="float32")
        assert result["VAL"].dtype == np.float32

    def test_set_float16(self):
        df = pd.DataFrame({"VAL": [1.0, 2.0, 3.0]})
        result = set_precision(df, precision="float16")
        assert result["VAL"].dtype == np.float16

    def test_optimize_memory(self):
        df = pd.DataFrame(
            {
                "INT": [1, 2, 3],
                "FLOAT": [1.0, 2.0, 3.0],
            }
        )
        result = optimize_memory(df)
        # Should use smaller types
        assert (
            result["INT"].dtype != np.int64
            or result["FLOAT"].dtype != np.float64
        )
