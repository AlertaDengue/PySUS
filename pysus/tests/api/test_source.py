"""Tests for pysus.api._impl.source — the origin/source primitive."""

from unittest.mock import AsyncMock, patch

import pytest
from pysus.api._impl.source import (
    APPLICABILITY,
    ORIGIN_CLIENT_MAP,
    ORIGIN_PREFIXES,
    _client_filter,
    fetch,
    valid_origins,
)


class TestOriginConstants:
    def test_valid_origins_exclude_ducklake(self):
        assert valid_origins() == ("FTP", "DADOSGOV", "SAUDE")

    def test_ducklake_not_an_origin(self):
        assert "DUCKLAKE" not in valid_origins()
        assert "DuckLake" not in valid_origins()

    def test_prefixes(self):
        assert ORIGIN_PREFIXES["FTP"] == "public/data/ftp/"
        assert ORIGIN_PREFIXES["DADOSGOV"] == "public/data/dadosgov/"
        assert ORIGIN_PREFIXES["SAUDE"] == "public/data/saude/"

    def test_client_map(self):
        assert ORIGIN_CLIENT_MAP["FTP"] == "ftp"
        assert ORIGIN_CLIENT_MAP["DADOSGOV"] == "dadosgov"
        assert "SAUDE" not in ORIGIN_CLIENT_MAP


class TestClientFilter:
    def test_none(self):
        assert _client_filter(None) is None

    def test_ftp(self):
        assert _client_filter("FTP") == "FTP"

    def test_dadosgov(self):
        assert _client_filter("DADOSGOV") == "DadosGov"

    def test_case_insensitive(self):
        assert _client_filter("ftp") == "FTP"
        assert _client_filter("dadosgov") == "DadosGov"

    def test_ducklake(self):
        assert _client_filter("DUCKLAKE") == "DuckLake"

    def test_unknown(self):
        assert _client_filter("NOPE") is None


class TestApplicability:
    def test_ftp_set(self):
        names = APPLICABILITY["FTP"]
        assert {"sinan", "sinasc", "sim", "sih", "sia", "pni"} <= names
        assert "arboviroses" not in names

    def test_dadosgov_omits_unpublished(self):
        names = APPLICABILITY["DADOSGOV"]
        # CKAN does not publish these → omitted (not exposed-and-405)
        assert {"sinan", "sim", "sinasc", "cnes", "pni", "covid19"} <= names
        assert "sih" not in names
        assert "sia" not in names
        assert "ciha" not in names
        assert "ibge" not in names

    def test_saude_themes(self):
        names = APPLICABILITY["SAUDE"]
        assert {"arboviroses", "vacinacao", "vigilancia_meio_ambiente"} <= names
        assert "sinan" not in names


class TestFetchRouting:
    def test_catalog_default_routes_to_ducklake(self):
        with patch(
            "pysus.api._impl.source._fetch_catalog",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_cat:
            fetch("sinan", year=2020, show_progress=False)
            mock_cat.assert_awaited_once()
            assert mock_cat.call_args.args[1] == "sinan"

    def test_source_origin_default_requires_origin(self):
        from pysus.api.errors import ValidationError

        with pytest.raises(ValidationError):
            fetch("sinan", source="origin", year=2020)

    def test_invalid_source_rejected(self):
        from pysus.api.errors import ValidationError

        with pytest.raises(ValidationError):
            fetch("sinan", source="bogus", year=2020)
