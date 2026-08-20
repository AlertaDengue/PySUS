"""Tests for pysus.management.records: identity keys and normalization."""

from datetime import datetime

from pysus.management.records import (
    DOWNLOAD_PRIORITY,
    FileComparison,
    FileRecord,
    IdentityKey,
    SyncOutcome,
    SyncReport,
    base_stem,
    canonical_dataset,
    canonical_group,
    compose_s3_key,
    format_of,
    parquet_key,
    stem_of,
)


class TestComposeS3Key:
    def test_full_attributes(self):
        key = compose_s3_key(
            origin="ftp",
            dataset="SIA",
            name="PAAC2501.dbc",
            group="PA",
            year=2025,
            month=1,
            state="AC",
        )
        assert key == "public/data/ftp/sia/PA/2025/01/AC/PAAC2501.parquet"

    def test_missing_month_state_defaults_br(self):
        key = compose_s3_key(
            origin="ftp",
            dataset="SINAN",
            name="DENGBR25.dbc",
            group="DENG",
            year=2025,
        )
        assert key == "public/data/ftp/sinan/DENG/2025/_/BR/DENGBR25.parquet"

    def test_missing_group_and_year(self):
        key = compose_s3_key(
            origin="dadosgov",
            dataset="SINAN",
            name="dados_tuberculose.csv",
        )
        assert (
            key == "public/data/dadosgov/sinan/_/_/_/BR/"
            "dados_tuberculose.parquet"
        )

    def test_month_zero_padded(self):
        key = compose_s3_key(
            origin="ftp",
            dataset="SIH",
            name="RDAC2502.dbc",
            group="RD",
            year=2025,
            month=2,
            state="AC",
        )
        assert key == "public/data/ftp/sih/RD/2025/02/AC/RDAC2502.parquet"

    def test_dataset_lowercased(self):
        key = compose_s3_key(
            origin="FTP",
            dataset="SINAN",
            name="DENGBR25.dbc",
        )
        assert key.startswith("public/data/ftp/sinan/")

    def test_csv_zip_and_dbc_share_key(self):
        a = compose_s3_key(
            origin="ftp",
            dataset="SINAN",
            name="DENGBR25.dbc",
            group="DENG",
            year=2025,
        )
        b = compose_s3_key(
            origin="dadosgov",
            dataset="SINAN",
            name="DENGBR25.csv.zip",
            group="DENG",
            year=2025,
        )
        assert a.split("/")[-1] == b.split("/")[-1] == "DENGBR25.parquet"


class TestStemOf:
    def test_dotted_format_zip(self):
        assert stem_of("DENGBR25.csv.zip") == "dengbr25"

    def test_single_extension(self):
        assert stem_of("DENGBR25.dbc") == "dengbr25"

    def test_underscore_format_token(self):
        assert stem_of("Mortalidade_Geral_2022_csv.zip") == (
            "mortalidade_geral_2022"
        )

    def test_json_variant_matches_csv_variant(self):
        assert stem_of("Mortalidade_Geral_2022_json.zip") == stem_of(
            "Mortalidade_Geral_2022_csv.zip"
        )

    def test_parquet_is_identity(self):
        assert stem_of("DENGBR25.parquet") == "dengbr25"

    def test_plain_csv(self):
        assert stem_of("dados_aids_hiv.csv") == "dados_aids_hiv"


class TestBaseStem:
    def test_case_preserved(self):
        assert base_stem("CHIKBR15.csv.zip") == "CHIKBR15"

    def test_dbc(self):
        assert base_stem("PFMS0508.dbc") == "PFMS0508"

    def test_underscore_token(self):
        assert base_stem("Mortalidade_Geral_2022_csv.zip") == (
            "Mortalidade_Geral_2022"
        )


class TestParquetKey:
    def test_no_format_token(self):
        assert parquet_key("CHIKBR15.csv.zip") == "CHIKBR15.parquet"

    def test_json_variant(self):
        assert parquet_key("Mortalidade_Geral_2022_json.parquet") == (
            "Mortalidade_Geral_2022.parquet"
        )

    def test_already_parquet(self):
        assert parquet_key("DENGBR25.parquet") == "DENGBR25.parquet"

    def test_dbc(self):
        assert parquet_key("PFMS0508.dbc") == "PFMS0508.parquet"


class TestFormatOf:
    def test_csv_zip(self):
        assert format_of("DENGBR25.csv.zip") == "csv.zip"

    def test_dbc(self):
        assert format_of("DENGBR25.dbc") == "dbc"

    def test_parquet(self):
        assert format_of("DENGBR25.parquet") == "parquet"

    def test_unknown(self):
        assert format_of("no_extension_name") == "unknown"


class TestCanonical:
    def test_dataset(self):
        assert canonical_dataset("sinan") == "SINAN"

    def test_group_none(self):
        assert canonical_group(None) is None

    def test_group(self):
        assert canonical_group("deng") == "DENG"


class TestFileRecord:
    def _record(self, **kwargs):
        defaults = {
            "origin": "ftp",
            "dataset": "SINAN",
            "name": "DENGBR25.dbc",
            "path": "/dissemin/publicos/SINAN/DADOS/PRELIM/DENGBR25.dbc",
            "size": 100,
            "modified": datetime(2026, 1, 1),
            "group": "DENG",
            "year": 2025,
            "state": "BR",
        }
        defaults.update(kwargs)
        return FileRecord(**defaults)

    def test_identity_key(self):
        key = self._record().identity_key()
        assert key.dataset == "SINAN"
        assert key.group == "DENG"
        assert key.year == 2025
        assert key.stem == "dengbr25"

    def test_format_inferred(self):
        record = self._record()
        assert record.format == "dbc"

    def test_csv_zip_matches_dbc_key(self):
        ftp = self._record()
        api = FileRecord(
            origin="dadosgov",
            dataset="sinan",
            name="DENGBR25.csv.zip",
            path="https://example.com/DENGBR25.csv.zip",
            group="deng",
            year=2025,
            state="br",
        )
        assert ftp.identity_key() == api.identity_key()

    def test_roundtrip(self):
        record = self._record(sha256="abc", rows=10)
        assert FileRecord.from_dict(record.to_dict()) == record

    def test_download_priority(self):
        assert DOWNLOAD_PRIORITY == ("ducklake", "ftp", "dadosgov", "saude")


class TestIdentityKeyAsTuple:
    def test_returns_six_elements(self):
        key = IdentityKey(
            dataset="SINAN",
            group="DENG",
            year=2025,
            month=1,
            state="RJ",
            stem="dengbr25",
        )
        t = key.as_tuple()
        assert len(t) == 6
        assert t == ("SINAN", "DENG", 2025, 1, "RJ", "dengbr25")


class TestFileComparisonFormats:
    def test_formats_property(self):
        ftp = FileRecord(
            origin="ftp",
            dataset="SINAN",
            name="DENGBR25.dbc",
            path="/x",
            group="DENG",
            year=2025,
        )
        api = FileRecord(
            origin="dadosgov",
            dataset="SINAN",
            name="DENGBR25.csv.zip",
            path="/y",
            group="DENG",
            year=2025,
        )
        comp = FileComparison(key=ftp.identity_key(), records=[ftp, api])
        assert comp.formats == {"dbc", "csv.zip"}

    def test_formats_empty_format_returns_unknown(self):
        rec = FileRecord(
            origin="ftp",
            dataset="X",
            name="X",
            path="/x",
            format=None,
        )
        comp = FileComparison(key=rec.identity_key(), records=[rec])
        assert comp.formats == {"unknown"}


class TestBestRecordNone:
    def test_returns_none_when_no_matching_origin(self):
        ftp = FileRecord(
            origin="ftp",
            dataset="X",
            name="X.dbc",
            path="/x",
        )
        comp = FileComparison(key=ftp.identity_key(), records=[ftp])
        result = comp.best_record(priorities=("alien",))
        assert result is None


class TestNeedsToken:
    def test_only_on_dadosgov_needs_token(self):
        rec = FileRecord(
            origin="dadosgov",
            dataset="X",
            name="X.csv",
            path="/x",
        )
        comp = FileComparison(key=rec.identity_key(), records=[rec])
        assert comp.needs_token is True

    def test_on_ftp_and_dadosgov_does_not_need_token(self):
        ftp = FileRecord(
            origin="ftp",
            dataset="X",
            name="X.dbc",
            path="/x",
        )
        api = FileRecord(
            origin="dadosgov",
            dataset="X",
            name="X.csv.zip",
            path="/y",
        )
        comp = FileComparison(key=ftp.identity_key(), records=[ftp, api])
        assert comp.needs_token is False


class TestFileComparisonToDict:
    def test_to_dict(self):
        ftp = FileRecord(
            origin="ftp",
            dataset="SINAN",
            name="DENGBR25.dbc",
            path="/x",
            group="DENG",
            year=2025,
        )
        comp = FileComparison(key=ftp.identity_key(), records=[ftp])
        d = comp.to_dict()
        assert "key" in d
        assert d["origins"] == ["ftp"]
        assert "records" in d
        assert len(d["records"]) == 1


class TestSyncReport:
    def _report(self):
        key = IdentityKey(
            dataset="X",
            group=None,
            year=2025,
            month=None,
            state=None,
            stem="x",
        )
        return SyncReport(
            outcomes=[
                SyncOutcome(key=key, origin="ftp", status="uploaded"),
                SyncOutcome(key=key, origin="ftp", status="skipped"),
                SyncOutcome(key=key, origin="ftp", status="failed"),
                SyncOutcome(key=key, origin="dadosgov", status="needs_token"),
            ]
        )

    def test_uploaded(self):
        r = self._report()
        assert len(r.uploaded) == 1
        assert r.uploaded[0].status == "uploaded"

    def test_skipped(self):
        r = self._report()
        assert len(r.skipped) == 1

    def test_failed(self):
        r = self._report()
        assert len(r.failed) == 1

    def test_needs_token(self):
        r = self._report()
        assert len(r.needs_token) == 1

    def test_summary(self):
        r = self._report()
        s = r.summary()
        assert s["total"] == 4
        assert s["uploaded"] == 1
        assert s["skipped"] == 1
        assert s["failed"] == 1
        assert s["needs_token"] == 1
