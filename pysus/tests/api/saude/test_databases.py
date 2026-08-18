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
    def test_19_specs_registered(self):
        assert len(DATASET_SPECS) == 19

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
                "VIGILANCIAMEIOAMBIENTE",
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
        spec = SPECS_BY_NAME["VIGILANCIAMEIOAMBIENTE"]
        assert spec.matches("mpox")
        assert not spec.matches("sisagua-controle-semestral")


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
        spec = spec_for("mpox", ("vigilancia-e-meio-ambiente",))
        assert spec is not None
        assert spec.name == "VIGILANCIAMEIOAMBIENTE"

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


class TestDatasetSpecIsFrozen:
    def test_frozen(self):
        spec = SPECS_BY_NAME["BNAFAR"]
        with pytest.raises(FrozenInstanceError):
            spec.name = "OTHER"  # type: ignore[misc]
