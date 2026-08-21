"""Tests for pysus.api.export module."""

import json

import pandas as pd
import pytest


def make_test_df():
    return pd.DataFrame(
        {
            "UF": ["RJ", "SP"],
            "IDADE": [25, 30],
            "VALOR": [100.0, 200.0],
        }
    )


class TestToCSV:
    def test_basic(self, tmp_path):
        from pysus.api.export.csv_excel import to_csv

        df = make_test_df()
        result = to_csv(df, tmp_path / "out.csv")
        assert result.exists()
        loaded = pd.read_csv(result)
        assert len(loaded) == 2

    def test_with_metadata(self, tmp_path):
        from pysus.api.export.csv_excel import to_csv

        df = make_test_df()
        to_csv(df, tmp_path / "out.csv", metadata={"source": "DATASUS"})
        meta_path = tmp_path / "out.metadata.json"
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text())
        assert meta["source"] == "DATASUS"

    def test_creates_parent_dirs(self, tmp_path):
        from pysus.api.export.csv_excel import to_csv

        df = make_test_df()
        to_csv(df, tmp_path / "sub" / "out.csv")
        assert (tmp_path / "sub" / "out.csv").exists()


class TestToExcel:
    def test_basic(self, tmp_path):
        pytest.importorskip("openpyxl", reason="openpyxl not installed")
        from pysus.api.export.csv_excel import to_excel

        df = make_test_df()
        result = to_excel(df, tmp_path / "out.xlsx")
        assert result.exists()

    def test_with_metadata_sheet(self, tmp_path):
        pytest.importorskip("openpyxl", reason="openpyxl not installed")
        from pysus.api.export.csv_excel import to_excel

        df = make_test_df()
        to_excel(df, tmp_path / "out.xlsx", metadata={"source": "DATASUS"})
        loaded = pd.ExcelFile(tmp_path / "out.xlsx")
        assert "Metadata" in loaded.sheet_names


class TestToGeoJSON:
    def test_basic(self, tmp_path):
        from pysus.api.export.geojson import to_geojson

        df = pd.DataFrame(
            {
                "LATITUDE": [-22.9, -23.5],
                "LONGITUDE": [-43.2, -46.6],
                "UF": ["RJ", "SP"],
            }
        )
        result = to_geojson(df, tmp_path / "out.geojson")
        assert result.exists()
        geojson = json.loads(result.read_text())
        assert geojson["type"] == "FeatureCollection"
        assert len(geojson["features"]) == 2
        assert geojson["features"][0]["geometry"]["type"] == "Point"

    def test_filters_null_coords(self, tmp_path):
        from pysus.api.export.geojson import to_geojson

        df = pd.DataFrame(
            {
                "LATITUDE": [-22.9, None],
                "LONGITUDE": [-43.2, None],
            }
        )
        result = to_geojson(df, tmp_path / "out.geojson")
        geojson = json.loads(result.read_text())
        assert len(geojson["features"]) == 1

    def test_with_properties(self, tmp_path):
        from pysus.api.export.geojson import to_geojson

        df = pd.DataFrame(
            {
                "LATITUDE": [-22.9],
                "LONGITUDE": [-43.2],
                "UF": ["RJ"],
            }
        )
        result = to_geojson(df, tmp_path / "out.geojson", properties=["UF"])
        geojson = json.loads(result.read_text())
        assert geojson["features"][0]["properties"]["UF"] == "RJ"


class TestToSQL:
    def test_basic_duckdb(self):
        from pysus.api.export.sql import to_sql

        df = make_test_df()
        ddl = to_sql(df, "my_table")
        assert "CREATE TABLE my_table" in ddl
        assert "UF" in ddl
        assert "VARCHAR" in ddl

    def test_mysql(self):
        from pysus.api.export.sql import to_sql

        df = make_test_df()
        ddl = to_sql(df, "my_table", dialect="mysql")
        assert "TEXT" in ddl

    def test_postgresql(self):
        from pysus.api.export.sql import to_sql

        df = make_test_df()
        ddl = to_sql(df, "my_table", dialect="postgresql")
        assert "TEXT" in ddl

    def test_sqlite(self):
        from pysus.api.export.sql import to_sql

        df = make_test_df()
        ddl = to_sql(df, "my_table", dialect="sqlite")
        assert "INTEGER" in ddl or "TEXT" in ddl
