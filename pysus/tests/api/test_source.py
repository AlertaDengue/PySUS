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


class TestOriginNamespaces:
    """Verify the public origin namespace modules."""

    @pytest.fixture()
    def import_pysus(self):
        import pysus  # noqa: F401

        return pysus

    def test_namespaces_registered(self, import_pysus):
        assert hasattr(import_pysus, "ftp")
        assert hasattr(import_pysus, "dadosgov")
        assert hasattr(import_pysus, "saude")

    def test_from_import_style(self):
        from pysus.dadosgov import sinasc
        from pysus.ftp import sinan
        from pysus.saude import arboviroses

        assert sinan.__name__ == "sinan"
        assert sinasc.__name__ == "sinasc"
        assert arboviroses.__name__ == "arboviroses"

    def test_ftp_binds_origin(self):
        import pysus

        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = []
            pysus.ftp.sinan(disease="deng", year=2017, show_progress=False)
            kwargs = mock_fetch.call_args.kwargs
            assert kwargs["origin"] == "FTP"
            assert kwargs["source"] == "catalog"

    def test_saude_binds_origin(self):
        import pysus

        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = []
            pysus.saude.arboviroses(show_progress=False)
            kwargs = mock_fetch.call_args.kwargs
            # Saude flat functions hardcode origin internally
            assert kwargs["origin"] == "Saude"
            assert kwargs["source"] == "catalog"

    def test_rejects_explicit_origin(self):
        import pysus
        from pysus.api.errors import PySUSError

        with pytest.raises(PySUSError):
            pysus.ftp.sinan(disease="deng", year=2017, origin="DadosGov")

    def test_discovery_names_present(self):
        import pysus

        for mod in (pysus.ftp, pysus.dadosgov, pysus.saude):
            for name in ("list_files", "info", "get_origin_meta"):
                assert hasattr(mod, name)

    def test_get_origin_meta(self):
        import pysus

        meta = pysus.ftp.get_origin_meta()
        assert meta["origin"] == "FTP"
        assert "sinan" in meta["fetchers"]

    def test_docstrings_present(self):
        import pysus

        assert pysus.ftp.__doc__
        assert pysus.dadosgov.__doc__
        assert pysus.saude.__doc__
        assert "origin" in pysus.ftp.sinan.__doc__.lower()
        assert pysus.ftp.list_files.__doc__
        assert pysus.saude.info.__doc__
        assert pysus.ftp.get_origin_meta.__doc__

    def test_dadosgov_omits_sih_sia_ciha_ibge(self):
        import pysus

        for name in ("sih", "sia", "ciha", "ibge"):
            assert not hasattr(pysus.dadosgov, name), name
