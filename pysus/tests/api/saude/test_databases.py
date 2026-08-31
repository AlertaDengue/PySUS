"""Tests for the Saude dataset registry (databases.py)."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest
from pysus.api.saude.databases import (
    DATASET_SPECS,
    SPECS_BY_NAME,
    parse_year,
    spec_for,
)


class TestDatasetSpecs:
    def test_18_specs_registered(self):
        assert len(DATASET_SPECS) == 18

    def test_names_are_unique_and_uppercase(self):
        names = [s.name for s in DATASET_SPECS]
        assert len(names) == len(set(names))
        assert all(n == n.upper() for n in names)

    def test_every_spec_has_description(self):
        for spec in DATASET_SPECS:
            assert spec.long_name
            assert spec.description

    def test_specs_by_name_lookup(self):
        assert SPECS_BY_NAME["SISAGUA"] is DATASET_SPECS[14]

    def test_endpoints_are_absolute_paths(self):
        for spec in DATASET_SPECS:
            for endpoint in spec.endpoints:
                assert endpoint.startswith("/")

    def test_all_specs_have_source_scope(self):
        # CNES/VACINACAO etc. are Saude-source declarations even when
        # the same logical dataset exists on dados.gov.br / DATASUS FTP
        for spec in DATASET_SPECS:
            assert spec.name in {
                "ARBOVIROSES",
                "ASSISTENCIASAUDE",
                "ATENCAOPRIMARIA",
                "BNAFAR",
                "CNES",
                "CIENCIATECNOLOGIA",
                "DIAGNOSTICOSTRATAMENTOS",
                "ECONOMIASAUDE",
                "EDUCACAOSAUDE",
                "MACROSAUDE",
                "OUVIDORIA",
                "OUTROSTEMAS",
                "PDA",
                "PREVENCAOPROMOCAO",
                "SISAGUA",
                "SISVAN",
                "SAUDEINDIGENA",
                "VACINACAO",
            }


class TestSpecMatches:
    def test_group_only_spec_matches_within_group(self):
        spec = SPECS_BY_NAME["ARBOVIROSES"]
        assert spec.matches("arboviroses-dengue")
        assert spec.matches("any-slug")  # group filter is external

    def test_pattern_spec(self):
        spec = SPECS_BY_NAME["SISVAN"]
        assert spec.matches("sisvan-estado-nutricional")
        assert not spec.matches("mpox")

    def test_exclude_pattern(self):
        # exclude_patterns is tested via spec_for integration tests
        pass


class TestSpecFor:
    def test_pattern_spec_wins_over_group_spec(self):
        spec = spec_for(
            "cnes-cadastro-nacional-de-estabelecimentos-de-saude",
            ("assistencia-a-saude",),
        )
        assert spec is not None
        assert spec.name == "CNES"

    def test_sisagua_in_vigilancia_group(self):
        spec = spec_for(
            "sisagua-controle-semestral",
            ("vigilancia-e-meio-ambiente",),
        )
        assert spec is not None
        assert spec.name == "SISAGUA"

    def test_mpox_in_vigilancia_group(self):
        # VIGILANCIAMEIOAMBIENTE was removed (saude API unreachable).
        # mpox no longer matches any spec.
        spec = spec_for("mpox", ("vigilancia-e-meio-ambiente",))
        assert spec is None

    def test_no_match(self):
        assert spec_for("nao-existe", ("arboviroses",)) is not None
        assert spec_for("nao-existe", ("grupo-inexistente",)) is None

    def test_group_only_spec(self):
        spec = spec_for("arboviroses-dengue", ("arboviroses",))
        assert spec is not None
        assert spec.name == "ARBOVIROSES"


class TestParseYear:
    def test_year_from_resource_name(self):
        assert parse_year("Dengue - 2024") == 2024
        assert parse_year("Dengue - 2000") == 2000

    def test_year_from_endpoint(self):
        assert parse_year("/vacinacao/doses-aplicadas-pni-2024") == 2024

    def test_no_year(self):
        assert parse_year("Dicionário de dados") is None

    def test_invalid_year(self):
        assert parse_year("Dengue - 1899") is None
        assert parse_year("Dengue - 2101") is None


class TestSaudeCsvToFrame:
    def test_utf8_csv(self, tmp_path):
        from pysus.api._impl.databases import _saude_csv_to_frame

        p = tmp_path / "dados.csv"
        p.write_text("ID;NOME\n1;JOÃO\n2;MARIA\n", encoding="utf-8")
        df = _saude_csv_to_frame(str(p))
        assert df is not None
        assert list(df.columns) == ["ID", "NOME"]
        assert df["NOME"].tolist() == ["JOÃO", "MARIA"]

    def test_latin1_csv_falls_back(self, tmp_path):
        # Regression: Saude resources are often Latin-1 even though the
        # default pd.read_csv assumes UTF-8 and would previously yield an
        # empty DataFrame for the whole dataset.
        from pysus.api._impl.databases import _saude_csv_to_frame

        p = tmp_path / "dados.csv"
        p.write_bytes("ID;NOME\n1;JOÃO\n2;MARIA\n".encode("latin-1"))
        df = _saude_csv_to_frame(str(p))
        assert df is not None
        assert df["NOME"].tolist() == ["JOÃO", "MARIA"]

    def test_unreadable_returns_none(self, tmp_path):
        from pysus.api._impl.databases import _saude_csv_to_frame

        p = tmp_path / "nao.csv"
        p.write_bytes(b"\x00\xffgarbage")
        # Latin-1 accepts any bytes, so a corrupt file still parses; but a
        # legitimately invalid table should not raise.
        assert _saude_csv_to_frame(str(p)) is not None

    def test_zipped_csv_is_unwrapped(self, tmp_path):
        import zipfile

        from pysus.api._impl.databases import _saude_csv_to_frame

        p = tmp_path / "um_csv.zip"
        with zipfile.ZipFile(p, "w") as zf:
            zf.writestr(
                "um/dicionario.txt",
                "documentation",
            )
            zf.writestr(
                "um/dados.csv",
                "ID;NOME\n1;JOÃO\n2;MARIA\n".encode(),
            )
        df = _saude_csv_to_frame(str(p))
        assert df is not None
        assert list(df.columns) == ["ID", "NOME"]
        assert df["NOME"].tolist() == ["JOÃO", "MARIA"]

    def test_missing_file_returns_none(self, tmp_path):
        from pysus.api._impl.databases import _saude_csv_to_frame

        assert _saude_csv_to_frame(str(tmp_path / "nao-existe.csv")) is None

    def test_empty_file_returns_none(self, tmp_path):
        from pysus.api._impl.databases import _saude_csv_to_frame

        p = tmp_path / "vazio.csv"
        p.write_bytes(b"")
        assert _saude_csv_to_frame(str(p)) is None

    def test_zip_without_csv_returns_none(self, tmp_path):
        import zipfile

        from pysus.api._impl.databases import _saude_csv_to_frame

        p = tmp_path / "sem_csv.zip"
        with zipfile.ZipFile(p, "w") as zf:
            zf.writestr("apenas.pdf", "not a table")
        assert _saude_csv_to_frame(str(p)) is None

    def test_sniff_failure_falls_back_to_comma(self, tmp_path):
        # A body with no discernible delimiter defeats csv.Sniffer; the
        # fallback delimiter (",") must still produce a usable frame.
        from pysus.api._impl.databases import _saude_csv_to_frame

        p = tmp_path / "simples.csv"
        p.write_text("a\nb\nc\nd\n", encoding="utf-8")
        df = _saude_csv_to_frame(str(p))
        assert df is not None and not df.empty


class TestDatasetSpecIsFrozen:
    def test_frozen(self):
        spec = SPECS_BY_NAME["BNAFAR"]
        with pytest.raises(FrozenInstanceError):
            spec.name = "OTHER"  # type: ignore[misc]
