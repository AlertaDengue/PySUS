"""Tests for pysus.management.records: identity keys and normalization."""

from datetime import datetime

from pysus.management.records import (
    DOWNLOAD_PRIORITY,
    FileRecord,
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
        key = compose_s3_key(origin="FTP", dataset="SINAN", name="DENGBR25.dbc")
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
        assert DOWNLOAD_PRIORITY == ("ducklake", "ftp", "dadosgov")
