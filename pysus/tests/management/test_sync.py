"""Tests for pysus.management.sync key resolution (no network)."""

from unittest.mock import MagicMock

from pysus.management.sync import SyncEngine


class TestSyncEngine:
    def _engine(self):
        return SyncEngine(access_key="ak", secret_key="sk")

    def _file(self, client_name, dataset_name, basename, **attrs):
        file = MagicMock()
        file.client.name = client_name
        file.dataset.name = dataset_name
        file.basename = basename
        group = MagicMock()
        group.name = attrs.get("group")
        file.group = group
        file.year = attrs.get("year")
        file.month = attrs.get("month")
        file.state = attrs.get("state")
        return file

    def test_s3_key_ftp_dbc(self):
        engine = self._engine()
        file = self._file(
            "ftp",
            "SINAN",
            "DENGBR25.dbc",
            group="DENG",
            year=2025,
        )
        assert (
            engine.s3_key_for(file)
            == "public/data/ftp/sinan/DENG/2025/_/BR/DENGBR25.parquet"
        )

    def test_s3_key_dadosgov_csv_zip(self):
        engine = self._engine()
        file = self._file(
            "DadosGov",
            "SINAN",
            "DENGBR25.csv.zip",
            group="DENG",
            year=2025,
        )
        assert (
            engine.s3_key_for(file)
            == "public/data/dadosgov/sinan/DENG/2025/_/BR/DENGBR25.parquet"
        )

    def test_s3_key_dadosgov_json_variant_collides_with_csv(self):
        engine = self._engine()
        csv = self._file(
            "DadosGov",
            "SIM",
            "Mortalidade_Geral_2022_csv.zip",
            group="DO",
            year=2022,
        )
        jsn = self._file(
            "DadosGov",
            "SIM",
            "Mortalidade_Geral_2022.json.zip",
            group="DO",
            year=2022,
        )
        assert engine.s3_key_for(csv) == engine.s3_key_for(jsn)

    def test_s3_key_full_attributes(self):
        engine = self._engine()
        file = self._file(
            "ftp",
            "SIA",
            "PAAC2501.dbc",
            group="PA",
            year=2025,
            month=1,
            state="AC",
        )
        assert (
            engine.s3_key_for(file)
            == "public/data/ftp/sia/PA/2025/01/AC/PAAC2501.parquet"
        )

    def test_is_current(self):
        engine = self._engine()
        from datetime import datetime

        file = MagicMock()
        file.modify = datetime(2026, 1, 2)
        assert engine._is_current(file, datetime(2026, 1, 2))
        assert not engine._is_current(file, datetime(2026, 1, 1))
        assert not engine._is_current(file, None)

    def test_s3_is_stale_when_ftp_newer(self):
        from datetime import datetime

        from pysus.management.records import FileComparison, FileRecord

        ftp = FileRecord(
            origin="ftp",
            dataset="SINAN",
            name="DENGBR25.dbc",
            path="ftp/x",
            modified=datetime(2026, 6, 1),
            group="DENG",
            year=2025,
        )
        s3 = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="s3/x",
            modified=datetime(2026, 1, 1),
            source_modified=datetime(2026, 1, 1),
            group="DENG",
            year=2025,
        )
        comparison = FileComparison(key=ftp.identity_key(), records=[ftp, s3])
        assert SyncEngine._s3_is_stale(comparison)

    def test_s3_not_stale_when_equal(self):
        from datetime import datetime

        from pysus.management.records import FileComparison, FileRecord

        ftp = FileRecord(
            origin="ftp",
            dataset="SINAN",
            name="DENGBR25.dbc",
            path="ftp/x",
            modified=datetime(2026, 1, 1),
            group="DENG",
            year=2025,
        )
        s3 = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="s3/x",
            modified=datetime(2026, 1, 2),
            source_modified=datetime(2026, 1, 1),
            group="DENG",
            year=2025,
        )
        comparison = FileComparison(key=ftp.identity_key(), records=[ftp, s3])
        assert not SyncEngine._s3_is_stale(comparison)
