"""Tests for pysus.api.errors — friendly error hierarchy."""

import pytest
from pysus.api.errors import (
    AuthenticationError,
    CatalogError,
    ConnectionError,
    ConversionError,
    DownloadError,
    FormatError,
    ParseError,
    PySUSError,
    PySUSWarning,
    ValidationError,
    warn,
)


class TestPySUSError:
    def test_base_message(self):
        err = PySUSError("something broke")
        assert "something broke" in str(err)

    def test_hint_in_str(self):
        err = PySUSError("oops", hint="try again")
        assert "try again" in str(err)

    def test_docs_url_in_str(self):
        err = PySUSError("oops", docs_url="https://example.com")
        assert "https://example.com" in str(err)

    def test_box_rendering(self):
        err = PySUSError("test", hint="do this")
        rendered = str(err)
        assert "╔" in rendered
        assert "╚" in rendered
        assert "PySUS PySUSError" in rendered

    def test_default_hint_empty(self):
        err = PySUSError("msg")
        assert err.hint == ""

    def test_subclass_name_in_box(self):
        err = DownloadError("fail")
        assert "PySUS DownloadError" in str(err)


class TestSpecificErrors:
    def test_connection_error_has_hint(self):
        err = ConnectionError("timeout")
        assert "network" in err.hint.lower()

    def test_auth_error_has_hint(self):
        err = AuthenticationError("bad token")
        assert "DADOSGOV_TOKEN" in err.hint

    def test_download_error_has_hint(self):
        err = DownloadError("404")
        assert "network" in err.hint.lower()

    def test_catalog_error_has_hint(self):
        err = CatalogError("not init")
        assert "ducklake" in err.hint.lower()

    def test_parse_error_has_hint(self):
        err = ParseError("bad json")
        assert "corrupted" in err.hint.lower()

    def test_conversion_error_has_hint(self):
        err = ConversionError("no converter")
        assert "corrupted" in err.hint.lower()

    def test_validation_error_has_hint(self):
        err = ValidationError("missing param")
        assert "signature" in err.hint.lower()

    def test_format_error_has_hint(self):
        err = FormatError("unknown")
        assert "supported" in err.hint.lower()

    def test_all_are_pysus_error(self):
        for cls in [
            ConnectionError,
            AuthenticationError,
            DownloadError,
            CatalogError,
            ParseError,
            ConversionError,
            ValidationError,
            FormatError,
        ]:
            assert issubclass(cls, PySUSError)


class TestPySUSWarning:
    def test_is_user_warning(self):
        assert issubclass(PySUSWarning, UserWarning)

    def test_warn_function(self):
        with pytest.warns(PySUSWarning, match="test warning"):
            warn("test warning")
