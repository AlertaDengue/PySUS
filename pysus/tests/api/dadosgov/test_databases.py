"""Tests for pysus.api.dadosgov.databases."""

from typing import Any
from unittest.mock import patch

from pysus.api.dadosgov.client import DadosGov
from pysus.api.dadosgov.databases import (
    AVAILABLE_DATABASES,
    CNES,
    COVID19,
    MONTHS,
    PNI,
    SIA,
    SIM,
    SINAN,
    SINASC,
    _parse_year,
    _skip,
)

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def db_instance(db_class):
    return db_class(client=DadosGov())


# ---------------------------------------------------------------------------
# MONTHS
# ---------------------------------------------------------------------------


class TestMONTHS:
    def test_all_months_present(self):
        assert MONTHS == {
            "jan": 1,
            "fev": 2,
            "mar": 3,
            "abr": 4,
            "mai": 5,
            "jun": 6,
            "jul": 7,
            "ago": 8,
            "set": 9,
            "out": 10,
            "nov": 11,
            "dez": 12,
        }


# ---------------------------------------------------------------------------
# _parse_year
# ---------------------------------------------------------------------------


class TestParseYear:
    def test_valid_year(self):
        assert _parse_year("2024") == 2024

    def test_year_below_range(self):
        assert _parse_year("1969") is None

    def test_year_above_range(self):
        assert _parse_year("2101") is None

    def test_non_numeric(self):
        assert _parse_year("abcd") is None

    def test_boundary_low(self):
        assert _parse_year("1970") == 1970

    def test_boundary_high(self):
        assert _parse_year("2100") == 2100


# ---------------------------------------------------------------------------
# _skip
# ---------------------------------------------------------------------------


class TestSkip:
    def test_get_prefix(self):
        assert _skip("get_dados.csv") is True
        assert _skip("get_.pdf") is True

    def test_pdf_suffix(self):
        assert _skip("documento.pdf") is True

    def test_normal_file(self):
        assert _skip("dados.csv") is False

    def test_empty_string(self):
        assert _skip("") is False


# ---------------------------------------------------------------------------
# Base dataset test helpers
# ---------------------------------------------------------------------------


class BaseDatasetMixin:
    db_class: Any = None
    expected_name = ""
    expected_long_name = ""

    def test_name(self):
        ds = db_instance(self.db_class)
        assert ds.name == self.expected_name

    def test_long_name(self):
        ds = db_instance(self.db_class)
        assert ds.long_name == self.expected_long_name

    def test_description_is_string(self):
        ds = db_instance(self.db_class)
        assert isinstance(ds.description, str)
        assert len(ds.description) > 0

    def test_ids_are_strings(self):
        ds = db_instance(self.db_class)
        assert isinstance(ds.ids, list)
        for i in ds.ids:
            assert isinstance(i, str)
            assert len(i) > 0

    def test_formatter_skip_pdf(self):
        ds = db_instance(self.db_class)
        assert ds.formatter("document.pdf") == {
            "state": None,
            "year": None,
            "month": None,
        }

    def test_formatter_skip_get_prefix(self):
        ds = db_instance(self.db_class)
        assert ds.formatter("get_dados.csv") == {
            "state": None,
            "year": None,
            "month": None,
        }

    def test_formatter_unrecognised(self):
        ds = db_instance(self.db_class)
        assert ds.formatter("random_file.xyz") == {
            "state": None,
            "year": None,
            "month": None,
        }

    def test_formatter_exception_handler(self):
        ds = db_instance(self.db_class)
        with patch(
            "pysus.api.dadosgov.databases._skip", side_effect=ValueError("test")
        ):
            assert ds.formatter("anything.csv") == {
                "state": None,
                "year": None,
                "month": None,
            }


# ---------------------------------------------------------------------------
# CNES
# ---------------------------------------------------------------------------


class TestCNES(BaseDatasetMixin):
    db_class = CNES
    expected_name = "CNES"
    expected_long_name = "Cadastro Nacional de Estabelecimentos de Saúde"

    def test_formatter_valid_pattern(self):
        ds = db_instance(CNES)
        result = ds.formatter("arquivo_01-2024.csv")
        assert result == {"state": None, "year": 2024, "month": 1}

    def test_formatter_month_and_year(self):
        ds = db_instance(CNES)
        result = ds.formatter("dados_12-2023.csv")
        assert result == {"state": None, "year": 2023, "month": 12}


# ---------------------------------------------------------------------------
# PNI
# ---------------------------------------------------------------------------


class TestPNI(BaseDatasetMixin):
    db_class = PNI
    expected_name = "PNI"
    expected_long_name = "Programa Nacional de Imunizações"

    def test_formatter_valid_pattern(self):
        ds = db_instance(PNI)
        result = ds.formatter("vacinacao_jan_2024_csv.zip")
        assert result == {"state": None, "year": 2024, "month": 1}

    def test_formatter_different_month(self):
        ds = db_instance(PNI)
        result = ds.formatter("vacinacao_dez_2023_csv.zip")
        assert result == {"state": None, "year": 2023, "month": 12}

    def test_formatter_invalid_month(self):
        ds = db_instance(PNI)
        result = ds.formatter("vacinacao_xxx_2024_csv.zip")
        assert result == {"state": None, "year": 2024, "month": None}

    def test_formatter_uppercase_filename(self):
        ds = db_instance(PNI)
        result = ds.formatter("VACINACAO_JAN_2024_CSV.ZIP")
        assert result == {"state": None, "year": 2024, "month": 1}

    def test_group_aliases(self):
        ds = db_instance(PNI)
        p = "doses-aplicadas-pelo-programa-de-nacional-de-imunizacoes-pni"
        assert ds.group_aliases[p] == "DPNI"
        assert ds.group_aliases[f"{p}-2020"] == "DPNI"
        assert ds.group_aliases[f"dataset-{p}_2022"] == "DPNI"


# ---------------------------------------------------------------------------
# SIA
# ---------------------------------------------------------------------------


class TestSIA(BaseDatasetMixin):
    db_class = SIA
    expected_name = "SIA"
    expected_long_name = "Sistema de Informações Ambulatoriais"

    def test_formatter_year_pattern(self):
        ds = db_instance(SIA)
        result = ds.formatter("arquivo_2024_.csv")
        assert result == {"state": None, "year": 2024, "month": None}

    def test_formatter_month_year_pattern(self):
        ds = db_instance(SIA)
        result = ds.formatter("arquivo_jun-out_2024_.csv")
        assert result == {"state": None, "year": 2024, "month": None}

    def test_formatter_month_year_upper(self):
        ds = db_instance(SIA)
        result = ds.formatter("ARQUIVO_JUN-OUT_2024_.CSV")
        assert result == {"state": None, "year": 2024, "month": None}


# ---------------------------------------------------------------------------
# SINAN
# ---------------------------------------------------------------------------


class TestSINAN(BaseDatasetMixin):
    db_class = SINAN
    expected_name = "SINAN"
    expected_long_name = "Sistema de Informação de Agravos de Notificação"

    def test_formatter_dengue_pattern(self):
        ds = db_instance(SINAN)
        result = ds.formatter("DENGBR24.CSV.ZIP")
        assert result == {"state": "BR", "year": 2024, "month": None}

    def test_formatter_tuberculose_pattern(self):
        ds = db_instance(SINAN)
        result = ds.formatter("TUBEBR99.CSV.ZIP")
        assert result == {"state": "BR", "year": 1999, "month": None}

    def test_formatter_monkeypox_pattern(self):
        ds = db_instance(SINAN)
        result = ds.formatter("MPX_2023_OPENDATASUS.CSV.ZIP")
        assert result == {"state": None, "year": 2023, "month": None}

    def test_formatter_lowercase_gets_uppercased(self):
        ds = db_instance(SINAN)
        result = ds.formatter("dengbr24.csv.zip")
        assert result == {"state": "BR", "year": 2024, "month": None}

    def test_group_aliases(self):
        ds = db_instance(SINAN)
        aliases = ds.group_aliases
        assert aliases["arboviroses-dengue"] == "DENG"
        assert aliases["arboviroses-febre-de-chikungunya"] == "CHIK"
        assert aliases["arboviroses-zika-virus"] == "ZIKA"
        assert aliases["hanseniase"] == "HANS"
        assert aliases["dados-tuberculose"] == "TUBE"
        assert aliases["sifilis"] == "SIFA"


# ---------------------------------------------------------------------------
# SIM
# ---------------------------------------------------------------------------


class TestSIM(BaseDatasetMixin):
    db_class = SIM
    expected_name = "SIM"
    expected_long_name = "Sistema de Informação sobre Mortalidade"

    def test_formatter_mortalidade_geral(self):
        ds = db_instance(SIM)
        result = ds.formatter("Mortalidade_Geral_2024_csv.zip")
        assert result == {"state": None, "year": 2024, "month": None}

    def test_formatter_do_pattern(self):
        ds = db_instance(SIM)
        result = ds.formatter("DO24OPEN")
        assert result == {"state": None, "year": 2024, "month": None}

    def test_formatter_do_century_handling(self):
        ds = db_instance(SIM)
        result = ds.formatter("DO99OPEN")
        assert result == {"state": None, "year": 1999, "month": None}

    def test_group_aliases(self):
        ds = db_instance(SIM)
        assert ds.group_aliases["sim-1979-2019"] == "DO"


# ---------------------------------------------------------------------------
# SINASC
# ---------------------------------------------------------------------------


class TestSINASC(BaseDatasetMixin):
    db_class = SINASC
    expected_name = "SINASC"
    expected_long_name = "Sistema de Informações sobre Nascidos Vivos"

    def test_formatter_sinasc_pattern(self):
        ds = db_instance(SINASC)
        result = ds.formatter("SINASC_2024_csv.zip")
        assert result == {"state": None, "year": 2024, "month": None}

    def test_formatter_dnbr_pattern(self):
        ds = db_instance(SINASC)
        result = ds.formatter("DNBR2024_csv.zip")
        assert result == {"state": "BR", "year": 2024, "month": None}

    def test_group_aliases(self):
        ds = db_instance(SINASC)
        key = "sistema-de-informacao-sobre-nascidos-vivos-sinasc-1996-a-20201"
        assert ds.group_aliases[key] == "DN"


# ---------------------------------------------------------------------------
# COVID19
# ---------------------------------------------------------------------------


class TestCOVID19(BaseDatasetMixin):
    db_class = COVID19
    expected_name = "COVID19"
    expected_long_name = "Casos Confirmados de COVID-19"

    def test_formatter_csv_file(self):
        ds = db_instance(COVID19)
        assert ds.formatter("casos_covid.csv") == {
            "state": None,
            "year": None,
            "month": None,
        }

    def test_formatter_xlsx_file(self):
        ds = db_instance(COVID19)
        assert ds.formatter("casos_covid.xlsx") == {
            "state": None,
            "year": None,
            "month": None,
        }

    def test_formatter_other_file(self):
        ds = db_instance(COVID19)
        assert ds.formatter("casos_covid.json") == {
            "state": None,
            "year": None,
            "month": None,
        }

    def test_formatter_uppercase_xlsx(self):
        ds = db_instance(COVID19)
        assert ds.formatter("casos_covid.XLSX") == {
            "state": None,
            "year": None,
            "month": None,
        }

    def test_formatter_xlsx_prefixed_with_get(self):
        ds = db_instance(COVID19)
        assert ds.formatter("get_casos.xlsx") == {
            "state": None,
            "year": None,
            "month": None,
        }


# ---------------------------------------------------------------------------
# AVAILABLE_DATABASES
# ---------------------------------------------------------------------------


class TestAVAILABLEDATABASES:
    def test_contains_all_databases(self):
        expected = {CNES, PNI, SIA, SINAN, SIM, SINASC, COVID19}
        assert set(AVAILABLE_DATABASES) == expected

    def test_all_can_be_instantiated(self):
        for db_class in AVAILABLE_DATABASES:
            ds = db_class(client=DadosGov())
            assert ds.name is not None
            assert ds.long_name is not None
