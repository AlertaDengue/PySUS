"""Tests for pysus.management.normalize."""

from unittest.mock import MagicMock

import pytest
from pysus.management.normalize import (
    _SCAN_PREFIXES,
    BucketNormalizer,
    CatalogPathFix,
    CatalogRowDelete,
    ObjectRename,
    formatter_for,
)

_SCHEMA = """
CREATE SCHEMA pysus;
CREATE TABLE pysus.dataset_groups (
    id INTEGER, dataset_id INTEGER, name VARCHAR, long_name VARCHAR,
    description VARCHAR
);
CREATE TABLE pysus.files (
    id INTEGER, dataset_id INTEGER, group_id INTEGER, path VARCHAR UNIQUE,
    size BIGINT, rows INTEGER, type VARCHAR, modified TIMESTAMP,
    origin_modified TIMESTAMP, origin_size BIGINT, origin_path VARCHAR,
    sha256 VARCHAR, source_sha256 VARCHAR, origin VARCHAR, format VARCHAR,
    year INTEGER, month INTEGER, state VARCHAR
);
CREATE TABLE pysus.file_columns (file_id INTEGER, column_id INTEGER);
"""


@pytest.fixture
def normalizer():
    client = MagicMock()
    normalizer = BucketNormalizer(access_key="ak", secret_key="sk")
    normalizer.client = client
    client.head_object.return_value = {"Metadata": {}}
    return normalizer


class TestFormatterFor:
    def test_ftp_formatter(self):
        formatter = formatter_for("ftp", "SINAN")
        assert formatter is not None
        parsed = formatter("DENGBR25.dbc")
        assert parsed["group"]["name"] == "DENG"
        assert parsed["year"] == 2025

    def test_dadosgov_formatter(self):
        formatter = formatter_for("dadosgov", "SIM")
        assert formatter is not None
        parsed = formatter("Mortalidade_Geral_2022_csv.zip")
        assert parsed["year"] == 2022

    def test_unknown_dataset(self):
        assert formatter_for("ftp", "NOPE") is None

    def test_cached(self):
        first = formatter_for("ftp", "CIHA")
        second = formatter_for("ftp", "CIHA")
        assert first is second


class TestSplitKey:
    def test_public_data_key(self):
        assert BucketNormalizer._split_key(
            "public/data/ftp/sinan/DENG/2025/_/BR/X.parquet"
        ) == ("ftp", "sinan")

    def test_non_public_key(self):
        assert BucketNormalizer._split_key("data/ftp/x") == (None, None)

    def test_short_key(self):
        assert BucketNormalizer._split_key("public/data") == (None, None)


class TestEnrich:
    def test_catalog_values_win(self, normalizer):
        enriched = normalizer._enrich(
            "ftp", "SINAN", "DENGBR25.dbc", "DENG", 2025, None, None
        )
        assert enriched == {
            "group": "DENG",
            "year": 2025,
            "month": None,
            "state": None,
        }

    def test_formatter_fills_gaps(self, normalizer):
        enriched = normalizer._enrich(
            "ftp", "SINAN", "DENGBR25.dbc", None, None, None, None
        )
        assert enriched["group"] == "DENG"
        assert enriched["year"] == 2025

    def test_legacy_group_replaced_by_formatter(self, normalizer):
        enriched = normalizer._enrich(
            "ftp", "CIHA", "CIHAMA2209.parquet", "Dados", 2022, 9, "MA"
        )
        assert enriched["group"] == "CIHA"

    def test_unknown_formatter_keeps_values(self, normalizer):
        enriched = normalizer._enrich(
            "ftp", "NOPE", "X.dbc", "G", 2020, 1, "AC"
        )
        assert enriched == {
            "group": "G",
            "year": 2020,
            "month": 1,
            "state": "AC",
        }


class TestSurveyRelayout:
    def test_survey_renames_and_deletes(self, normalizer, tmp_path):
        import duckdb as _duckdb

        con = _duckdb.connect(str(tmp_path / "catalog_sinan.duckdb"))
        con.execute(_SCHEMA)
        con.execute(
            "INSERT INTO pysus.files (id, dataset_id, group_id, path, size, "
            "rows, modified, origin_modified, origin_size, origin_path, "
            "sha256, year, month, state) VALUES (1, 8, NULL, "
            "'public/data/ftp/sinan/DENGBR25.parquet', 100, 5, "
            "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 50, "
            "'/ftp/DENGBR25.dbc', NULL, 2025, NULL, NULL)"
        )
        con.execute(
            "INSERT INTO pysus.files (id, dataset_id, group_id, path, size, "
            "rows, modified, origin_modified, origin_size, origin_path, "
            "sha256, year, month, state) VALUES (2, 8, NULL, "
            "'public/data/ftp/sinan/DENGBR25.dbc', 50, 5, "
            "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 50, "
            "'/ftp/DENGBR25.dbc', NULL, 2025, NULL, NULL)"
        )
        con.close()

        objects = {
            "public/data/ftp/sinan/DENGBR25.parquet",
            "public/data/ftp/sinan/DENGBR25.dbc",
        }
        plan = normalizer.survey_relayout(
            tmp_path / "catalog_sinan.duckdb", objects
        )
        assert len(plan.catalog_fixes) == 1
        fix = plan.catalog_fixes[0]
        assert fix.new_path == (
            "public/data/ftp/sinan/DENG/2025/_/BR/DENGBR25.parquet"
        )
        assert len(plan.object_renames) == 1
        assert len(plan.catalog_row_deletes) == 1

    def test_missing_object_rows_deleted(self, normalizer, tmp_path):
        import duckdb as _duckdb

        con = _duckdb.connect(str(tmp_path / "catalog_x.duckdb"))
        con.execute(_SCHEMA)
        con.execute(
            "INSERT INTO pysus.files (id, dataset_id, group_id, path, size, "
            "rows, modified, origin_modified, origin_size, origin_path, "
            "sha256, year, month, state) VALUES (1, 8, NULL, "
            "'public/data/ftp/sinan/DENGBR25.parquet', 100, 5, "
            "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 50, "
            "'/ftp/DENGBR25.dbc', NULL, 2025, NULL, NULL)"
        )
        con.close()

        plan = normalizer.survey_relayout(tmp_path / "catalog_x.duckdb", set())
        assert len(plan.catalog_fixes) == 0
        assert len(plan.catalog_row_deletes) == 1


class TestRelocateUncataloged:
    def test_relocates_with_formatter(self, normalizer):
        keys = {
            "public/data/ftp/sinan/DENGBR25.parquet",
            "public/data/dadosgov/sim/Mortalidade_Geral_2022.parquet",
        }
        plan = normalizer.relocate_uncataloged(keys, set())
        assert len(plan.object_renames) == 2
        targets = {r.new for r in plan.object_renames}
        assert "public/data/ftp/sinan/DENG/2025/_/BR/DENGBR25.parquet" in (
            targets
        )

    def test_unparsable_kept_with_placeholders(self, normalizer):
        plan = normalizer.relocate_uncataloged(
            {"public/data/ftp/sinan/weird-file-xyz.parquet"}, set()
        )
        assert len(plan.object_renames) == 1
        assert plan.object_renames[0].new == (
            "public/data/ftp/sinan/_/_/_/BR/weird-file-xyz.parquet"
        )

    def test_non_public_key_kept_raw(self, normalizer):
        plan = normalizer.relocate_uncataloged(
            {"data/ftp/sih/ERAC1101.parquet"}, set()
        )
        assert plan.object_renames == []
        assert len(plan.raw_objects) == 1


class TestCopySource:
    def test_large_object_skips_head(self, normalizer):
        assert (
            normalizer._copy_source("public/data/k", size=100000)
            == "public/data/k"
        )
        normalizer.client.head_object.assert_not_called()

    def test_alias_followed(self, normalizer):
        normalizer.client.head_object.side_effect = [
            {"Metadata": {"pysus-alias": "public/data/new"}},
            {"Metadata": {}},
        ]
        assert (
            normalizer._copy_source("public/data/old", size=10)
            == "public/data/new"
        )

    def test_head_error_returns_key(self, normalizer):
        normalizer.client.head_object.side_effect = Exception("boom")
        assert (
            normalizer._copy_source("public/data/k", size=10) == "public/data/k"
        )


class TestDoRelocate:
    def test_self_copy_skipped(self, normalizer):
        rename = ObjectRename(old="k", new="k")
        normalizer._do_relocate(rename, {"k": 10})
        normalizer.client.copy_object.assert_not_called()

    def test_relocate_copies_and_aliases(self, normalizer):
        normalizer.client.head_object.return_value = {"Metadata": {}}
        rename = ObjectRename(old="old", new="new")
        normalizer._do_relocate(rename, {"old": 100})
        normalizer.client.copy_object.assert_called_once()
        normalizer.client.put_object.assert_called_once()
        meta = normalizer.client.put_object.call_args.kwargs["Metadata"]
        assert meta == {"pysus-alias": "new"}


class TestApplyRenamesWithAliases:
    def test_dry_run_returns_empty(self, normalizer):
        aliases = normalizer.apply_renames_with_aliases(
            [ObjectRename(old="a", new="b")], dry_run=True
        )
        assert aliases == {}
        normalizer.client.copy_object.assert_not_called()

    def test_parallel_apply(self, normalizer):
        normalizer.client.head_object.return_value = {"Metadata": {}}
        renames = [
            ObjectRename(old=f"old{i}", new=f"new{i}") for i in range(10)
        ]
        aliases = normalizer.apply_renames_with_aliases(
            renames,
            dry_run=False,
            object_sizes={"old0": 100},
            workers=4,
        )
        assert set(aliases) == {f"old{i}" for i in range(10)}
        assert normalizer.client.copy_object.call_count == 10


class TestApplyObjects:
    def test_deletes(self, normalizer):
        normalizer.apply_objects([], ["k1", "k2"], dry_run=False)
        assert normalizer.client.delete_object.call_count == 2

    def test_dry_run(self, normalizer):
        normalizer.apply_objects([], ["k1"], dry_run=True)
        normalizer.client.delete_object.assert_not_called()


class TestSurveyCatalog:
    def test_survey_broken_and_raw(self, normalizer, tmp_path):
        import duckdb as _duckdb

        con = _duckdb.connect(str(tmp_path / "catalog_ciha.duckdb"))
        con.execute(_SCHEMA)
        con.execute(
            "INSERT INTO pysus.files (id, dataset_id, group_id, path, size, "
            "rows, modified, origin_modified, origin_size, origin_path, "
            "sha256, year, month, state) VALUES (1, 8, NULL, "
            "'public/data/ftp/ciha/CIHAAC2201.dbc', 100, 5, "
            "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 50, "
            "'/ftp/CIHAAC2201.dbc', NULL, 2022, 1, 'AC')"
        )
        con.close()

        normalizer._object_exists = lambda key: key.endswith(".dbc")
        fixes, deletes = normalizer.survey_catalog(
            tmp_path / "catalog_ciha.duckdb"
        )
        assert fixes == []
        assert deletes == []
        assert normalizer.raw_objects == ["public/data/ftp/ciha/CIHAAC2201.dbc"]


class TestSurveyObjects:
    def test_non_parquet_raw(self, normalizer):
        normalizer._list_objects = MagicMock(
            return_value=[("public/data/ftp/ciha/X.dbc", 100)]
        )
        renames, deletes = normalizer.survey_objects()
        assert renames == []
        assert normalizer.raw_objects.count(
            "public/data/ftp/ciha/X.dbc"
        ) == len(_SCAN_PREFIXES)

    def test_format_token_parquet(self, normalizer):
        def _listing(prefix):
            if prefix == "public/data/dadosgov/":
                return [("public/data/dadosgov/sinan/X.csv.parquet", 100)]
            return []

        normalizer._list_objects = MagicMock(side_effect=_listing)
        renames, deletes = normalizer.survey_objects()
        assert len(renames) == 1
        assert renames[0].new == "public/data/dadosgov/sinan/X.parquet"

    def test_collision_keeps_csv(self, normalizer):
        def _listing(prefix):
            if prefix == "public/data/dadosgov/":
                return [
                    ("public/data/dadosgov/sim/M_2022.csv.parquet", 200),
                    ("public/data/dadosgov/sim/M_2022.json.parquet", 100),
                ]
            return []

        normalizer._list_objects = MagicMock(side_effect=_listing)
        renames, deletes = normalizer.survey_objects()
        assert len(renames) == 1
        assert renames[0].old.endswith("csv.parquet")
        assert deletes == ["public/data/dadosgov/sim/M_2022.json.parquet"]


class TestApplyCatalog:
    def test_apply_fixes_and_deletes(self, normalizer, tmp_path):
        import duckdb as _duckdb

        path = tmp_path / "catalog_sinan.duckdb"
        con = _duckdb.connect(str(path))
        con.execute(_SCHEMA)
        con.execute(
            "INSERT INTO pysus.files (id, dataset_id, group_id, path, size, "
            "rows, modified, origin_modified, origin_size, origin_path, "
            "sha256, year, month, state) VALUES (1, 8, NULL, "
            "'public/data/ftp/sinan/DENGBR25.parquet', 100, 5, "
            "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 50, "
            "'/ftp/DENGBR25.dbc', NULL, 2025, NULL, NULL)"
        )
        con.close()

        normalizer.apply_catalog(
            path,
            [
                CatalogPathFix(
                    catalog="sinan",
                    old_path="public/data/ftp/sinan/DENGBR25.parquet",
                    new_path="public/data/ftp/sinan/DENG/2025/_/BR/"
                    "DENGBR25.parquet",
                )
            ],
            [
                CatalogRowDelete(
                    catalog="sinan",
                    path="public/data/ftp/sinan/DENGBR25.parquet",
                )
            ],
            dry_run=False,
        )

        con = _duckdb.connect(str(path), read_only=True)
        rows = con.execute("SELECT path FROM pysus.files").fetchall()
        con.close()
        expected = "public/data/ftp/sinan/DENG/2025/_/BR/DENGBR25.parquet"
        assert rows == [(expected,)]
