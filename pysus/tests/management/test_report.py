"""Tests for pysus.management.report."""

from unittest.mock import MagicMock

from pysus.management.records import FileRecord
from pysus.management.report import ComparisonReporter


def _record(origin, name, dataset="SINAN", year=2025, group=None):
    return FileRecord(
        origin=origin,
        dataset=dataset,
        name=name,
        path=f"{origin}/{dataset}/{name}",
        group=group,
        year=year,
        file=MagicMock(),
    )


class TestComparisonReporter:
    def test_all_three(self):
        records = [
            _record("ftp", "DENGBR25.dbc", group="DENG"),
            _record("dadosgov", "DENGBR25.csv.zip", group="DENG"),
            _record("ducklake", "DENGBR25.parquet", group="DENG"),
        ]
        reports = ComparisonReporter().report(records)
        assert len(reports) == 1
        assert reports[0].total == 1
        assert reports[0].on_all_three == 1

    def test_ftp_only(self):
        records = [
            _record("ftp", "DENGBR25.dbc", group="DENG"),
            _record("ducklake", "CHIKBR25.parquet", group="CHIK"),
        ]
        reports = ComparisonReporter().report(records)
        report = reports[0]
        assert report.total == 2
        assert report.ftp_only == 1
        assert report.s3_only == 1
        assert report.examples["ftp_only"] == ["SINAN/DENG/2025/-/dengbr25"]

    def test_dadosgov_only(self):
        records = [
            _record("dadosgov", "MPX_2024_OPENDATASUS.csv.zip"),
        ]
        reports = ComparisonReporter().report(records)
        assert reports[0].dadosgov_only == 1
        assert reports[0].examples["dadosgov_only"] == [
            "SINAN/-/2025/-/mpx_2024_opendatasus"
        ]

    def test_dadosgov_s3_pair(self):
        records = [
            _record("dadosgov", "dados_tuberculose.csv"),
            _record("ducklake", "dados_tuberculose.parquet"),
        ]
        reports = ComparisonReporter().report(records)
        assert reports[0].on_dadosgov_s3 == 1

    def test_per_dataset_split(self):
        records = [
            _record("ftp", "DENGBR25.dbc", dataset="SINAN"),
            _record("ftp", "DO25OPEN.dbc", dataset="SIM"),
        ]
        reports = ComparisonReporter().report(records)
        datasets = {r.dataset for r in reports}
        assert datasets == {"SIM", "SINAN"}

    def test_to_dict_serializable(self):
        import json

        records = [_record("ftp", "DENGBR25.dbc", group="DENG")]
        reports = ComparisonReporter().report(records)
        json.dumps([r.to_dict() for r in reports])
