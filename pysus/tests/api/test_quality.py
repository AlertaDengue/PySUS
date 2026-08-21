"""Tests for pysus.api.quality module."""

import pandas as pd
from pysus.api.quality import (
    column_stats,
    missing_values,
    profile_report,
    quality_score,
    validate_data,
)


def make_test_df():
    """Create a test DataFrame."""
    return pd.DataFrame(
        {
            "DT_NOTIFIC": ["20240101", "20240102", None, "20240104"],
            "IDADE": [25, 30, 45, None],
            "CS_SEXO": ["M", "F", "M", "F"],
            "CID": ["A01", "B02", "C03", "D04"],
            "VALOR": [100.0, None, 300.0, 400.0],
        }
    )


class TestMissingValues:
    def test_basic(self):
        df = make_test_df()
        result = missing_values(df)
        assert isinstance(result, pd.DataFrame)
        assert "column" in result.columns
        assert "missing_pct" in result.columns

    def test_with_threshold(self):
        df = make_test_df()
        result = missing_values(df, threshold=0.1)
        # Should only include columns with >10% missing
        assert len(result) <= len(df.columns)

    def test_with_group_by(self):
        df = make_test_df()
        result = missing_values(df, group_by="CS_SEXO")
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_empty_df(self):
        df = pd.DataFrame()
        result = missing_values(df)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


class TestValidateData:
    def test_basic(self):
        df = make_test_df()
        results = validate_data(df)
        assert isinstance(results, list)

    def test_age_validation(self):
        df = pd.DataFrame({"IDADE": [25, 30, 200, -5]})
        results = validate_data(df)
        age_results = [r for r in results if r.column == "IDADE"]
        assert len(age_results) == 1
        assert not age_results[0].passed

    def test_date_validation(self):
        df = pd.DataFrame({"DT_NOTIFIC": ["20240101", "invalid"]})
        results = validate_data(df)
        date_results = [r for r in results if r.column == "DT_NOTIFIC"]
        assert len(date_results) == 1
        assert not date_results[0].passed

    def test_custom_rules(self):
        df = pd.DataFrame({"VALOR": [10, 20, 300, 400]})
        rules = {"VALOR": {"type": "range", "min": 0, "max": 100}}
        results = validate_data(df, rules=rules)
        val_results = [r for r in results if r.column == "VALOR"]
        assert len(val_results) == 1
        assert not val_results[0].passed


class TestColumnStats:
    def test_basic(self):
        df = make_test_df()
        result = column_stats(df)
        assert isinstance(result, pd.DataFrame)
        assert "column" in result.columns
        assert "memory_bytes" in result.columns
        assert "null_pct" in result.columns
        assert "unique_count" in result.columns

    def test_sorted_by_memory(self):
        df = make_test_df()
        result = column_stats(df)
        assert result["memory_bytes"].is_monotonic_decreasing

    def test_empty_df(self):
        df = pd.DataFrame()
        result = column_stats(df)
        assert len(result) == 0


class TestQualityScore:
    def test_basic(self):
        df = make_test_df()
        result = quality_score(df)
        assert 0 <= result.overall <= 100
        assert 0 <= result.completeness <= 100
        assert 0 <= result.validity <= 100
        assert 0 <= result.consistency <= 100

    def test_complete_df(self):
        df = pd.DataFrame({"A": [1, 2, 3], "B": ["x", "y", "z"]})
        result = quality_score(df)
        assert result.overall == 100.0

    def test_empty_df(self):
        df = pd.DataFrame()
        result = quality_score(df)
        assert result.overall == 100.0

    def test_details(self):
        df = make_test_df()
        result = quality_score(df)
        assert "total_rows" in result.details
        assert "total_columns" in result.details
        assert "completeness_by_column" in result.details


class TestProfileReport:
    def test_text_report(self):
        df = make_test_df()
        result = profile_report(df, format="text")
        assert isinstance(result, str)
        assert "DATASUS" in result
        assert "Rows" in result

    def test_json_report(self):
        df = make_test_df()
        result = profile_report(df, format="json")
        assert isinstance(result, dict)
        assert "overview" in result
        assert "quality_score" in result

    def test_html_report(self):
        df = make_test_df()
        result = profile_report(df, format="html")
        assert isinstance(result, str)
        assert "<html>" in result

    def test_save_to_file(self, tmp_path):
        df = make_test_df()
        output = tmp_path / "report.txt"
        result = profile_report(df, output=output, format="text")
        assert output.exists()
        assert output.read_text() == result

    def test_save_json_to_file(self, tmp_path):
        df = make_test_df()
        output = tmp_path / "report.json"
        profile_report(df, output=output, format="json")
        assert output.exists()
