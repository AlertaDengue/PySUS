"""Tests for pysus.management.compare and inventory diffing."""

from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
from pysus.management.compare import (
    FORMAT_PREFERENCE,
    Comparator,
    _format_rank,
    content_fingerprint,
)
from pysus.management.inventory import Inventory
from pysus.management.records import FileComparison, FileRecord


def _record(origin, name, dataset="SINAN", group="DENG", year=2025, **kw):
    return FileRecord(
        origin=origin,
        dataset=dataset,
        name=name,
        path=f"{origin}/{dataset}/{name}",
        group=group,
        year=year,
        size=kw.pop("size", 100),
        modified=kw.pop("modified", datetime(2026, 1, 1)),
        file=kw.pop("file", MagicMock()),
        **kw,
    )


class TestComparator:
    def test_same_logical_file_groups_across_origins(self):
        records = [
            _record("ftp", "DENGBR25.dbc"),
            _record("dadosgov", "DENGBR25.csv.zip"),
            _record("ducklake", "DENGBR25.parquet", size=10),
        ]
        comparisons = Comparator().compare(records)
        assert len(comparisons) == 1
        assert comparisons[0].origins == {"ftp", "dadosgov", "ducklake"}

    def test_different_years_do_not_group(self):
        records = [
            _record("ftp", "DENGBR25.dbc", year=2025),
            _record("ftp", "DENGBR24.dbc", year=2024),
        ]
        comparisons = Comparator().compare(records)
        assert len(comparisons) == 2

    def test_csv_json_xml_triplet_deduped(self):
        records = [
            _record("dadosgov", "DENGBR25.csv.zip"),
            _record("dadosgov", "DENGBR25.json.zip"),
            _record("dadosgov", "DENGBR25.xml.zip"),
        ]
        comparisons = Comparator().compare(records)
        assert len(comparisons) == 1
        assert len(comparisons[0].records) == 1
        assert comparisons[0].records[0].format == "csv.zip"

    def test_best_record_priority_s3_first(self):
        ftp = _record("ftp", "DENGBR25.dbc")
        api = _record("dadosgov", "DENGBR25.csv.zip")
        s3 = _record("ducklake", "DENGBR25.parquet")
        comparison = FileComparison(
            key=ftp.identity_key(), records=[api, ftp, s3]
        )
        assert comparison.best_record() is s3
        assert comparison.is_on_s3

    def test_best_record_ftp_before_dadosgov(self):
        ftp = _record("ftp", "DENGBR25.dbc")
        api = _record("dadosgov", "DENGBR25.csv.zip")
        comparison = FileComparison(key=ftp.identity_key(), records=[api, ftp])
        assert comparison.best_record() is ftp
        assert not comparison.is_on_s3
        assert not comparison.only_on_dadosgov

    def test_only_on_dadosgov(self):
        api = _record("dadosgov", "DENGBR25.csv.zip")
        comparison = FileComparison(key=api.identity_key(), records=[api])
        assert comparison.only_on_dadosgov
        assert comparison.best_record() is api

    def test_state_none_vs_br_does_not_split(self):
        ftp = _record("ftp", "DENGBR25.dbc", state=None)
        api = _record("dadosgov", "DENGBR25.csv.zip", state="BR")
        comparisons = Comparator().compare([ftp, api])
        assert len(comparisons) == 1
        assert comparisons[0].origins == {"ftp", "dadosgov"}
        assert comparisons[0].key.state == "BR"

    def test_state_level_files_keep_own_groups_via_stem(self):
        a = _record("ftp", "DNRJ2401.dbc", state="RJ", year=2024)
        b = _record("ftp", "DNRS2401.dbc", state="RS", year=2024)
        comparisons = Comparator().compare([a, b])
        assert len(comparisons) == 2

    def test_group_none_vs_group_merges(self):
        ftp = _record("ftp", "ACBIBR07.dbc", group="ACBI")
        s3 = _record("ducklake", "ACBIBR07.parquet", group=None)
        comparisons = Comparator().compare([ftp, s3])
        assert len(comparisons) == 1
        assert comparisons[0].key.group == "ACBI"

    def test_month_none_vs_month_merges(self):
        ftp = _record("ftp", "PAAC2408.dbc", group="PA", month=8)
        api = _record("dadosgov", "PAAC2408.csv.zip", group="PA", month=None)
        comparisons = Comparator().compare([ftp, api])
        assert len(comparisons) == 1
        assert comparisons[0].key.month == 8

    def test_format_preference_csv_first(self):
        assert FORMAT_PREFERENCE[0] == "csv"

    def test_pick_returns_best_from_comparison(self):
        ftp = _record("ftp", "DENGBR25.dbc")
        api = _record("dadosgov", "DENGBR25.csv.zip")
        s3 = _record("ducklake", "DENGBR25.parquet")
        comparison = FileComparison(
            key=ftp.identity_key(), records=[api, ftp, s3]
        )
        comp = Comparator()
        assert comp.pick(comparison) is s3

    def test_pick_returns_none_for_unknown_origin(self):
        ftp = _record("ftp", "DENGBR25.dbc")
        comparison = FileComparison(key=ftp.identity_key(), records=[ftp])
        comp = Comparator(priorities=("unknown_origin",))
        assert comp.pick(comparison) is None


class TestFormatRank:
    def test_empty_string(self):
        assert _format_rank("") == len(FORMAT_PREFERENCE) + 1

    def test_unknown_string(self):
        assert _format_rank("unknown") == len(FORMAT_PREFERENCE) + 1

    def test_whitespace_only(self):
        assert _format_rank("  ") == len(FORMAT_PREFERENCE) + 1

    def test_unknown_format(self):
        assert _format_rank("xyzzy") == len(FORMAT_PREFERENCE)


class TestContentFingerprint:
    def test_same_content_same_fingerprint(self):
        a = pd.DataFrame({"col": [1, 2, 3]})
        b = pd.DataFrame({"col": [1, 2, 3]})
        assert content_fingerprint(a) == content_fingerprint(b)

    def test_different_rows_differ(self):
        a = pd.DataFrame({"col": [1, 2, 3]})
        b = pd.DataFrame({"col": [1, 2, 4]})
        assert content_fingerprint(a) != content_fingerprint(b)

    def test_column_order_invariant(self):
        a = pd.DataFrame({"x": [1], "y": [2]})
        b = pd.DataFrame({"y": [2], "x": [1]})
        assert content_fingerprint(a) == content_fingerprint(b)

    def test_large_dataframe_uses_even_spacing(self):
        big = pd.DataFrame({"col": list(range(2000))})
        small = pd.DataFrame({"col": list(range(2000))})
        assert content_fingerprint(big) == content_fingerprint(small)

    def test_large_dataframe_different_data_differs(self):
        a = pd.DataFrame({"col": list(range(2000))})
        b = pd.DataFrame({"col": list(range(2000, 4000))})
        assert content_fingerprint(a) != content_fingerprint(b)


class TestInventoryDiff:
    def _inventory(self):
        return Inventory(pysus=MagicMock())

    def test_diff_empty_previous(self):
        inventory = self._inventory()
        current = [
            _record("ftp", "A.dbc"),
            _record("ftp", "B.dbc"),
        ]
        diff = inventory.diff(None, current, "ftp")
        assert len(diff.added) == 2
        assert diff.removed == []
        assert diff.changed == []
        assert diff.has_changes

    def test_diff_added_removed_changed(self):
        inventory = self._inventory()
        previous = [
            _record("ftp", "A.dbc", size=100),
            _record("ftp", "B.dbc", size=100),
        ]
        current = [
            _record("ftp", "B.dbc", size=200),
            _record("ftp", "C.dbc", size=100),
        ]
        diff = inventory.diff(previous, current, "ftp")
        assert [r.name for r in diff.added] == ["C.dbc"]
        assert [r.name for r in diff.removed] == ["A.dbc"]
        assert len(diff.changed) == 1
        assert diff.changed_count == 3

    def test_snapshot_roundtrip(self, tmp_path):
        inventory = Inventory(pysus=MagicMock(), snapshot_dir=tmp_path)
        records = [_record("ftp", "A.dbc", sha256="deadbeef")]
        path = inventory.save_snapshot("ftp", records)
        loaded = inventory.load_snapshot("ftp")
        assert path.exists()
        assert loaded == records

    def test_load_missing_snapshot(self, tmp_path):
        inventory = Inventory(pysus=MagicMock(), snapshot_dir=tmp_path)
        assert inventory.load_snapshot("ftp") is None

    def test_load_corrupt_snapshot(self, tmp_path):
        inventory = Inventory(pysus=MagicMock(), snapshot_dir=tmp_path)
        path = inventory._snapshot_path("ftp")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{bad json")
        assert inventory.load_snapshot("ftp") is None

    def test_load_snapshot_missing_records_key(self, tmp_path):
        inventory = Inventory(pysus=MagicMock(), snapshot_dir=tmp_path)
        path = inventory._snapshot_path("ftp")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"origin": "ftp"}')
        assert inventory.load_snapshot("ftp") is None
