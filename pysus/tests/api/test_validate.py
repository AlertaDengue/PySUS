"""Tests for pysus.api.validate — input validation with suggestions."""

import pytest
from pysus.api.errors import ValidationError
from pysus.api.validate import (
    validate_choice,
    validate_dataset,
    validate_origin,
    validate_source,
)


class TestValidateChoice:
    def test_exact_match(self):
        result = validate_choice("SINAN", ["SINAN", "SIA", "SIM"])
        assert result == "SINAN"

    def test_case_insensitive(self):
        result = validate_choice("sinan", ["SINAN", "SIA", "SIM"])
        assert result == "SINAN"

    def test_typo_suggests(self):
        with pytest.raises(ValidationError, match="Did you mean"):
            validate_choice("sinam", ["SINAN", "SIA", "SIM"])

    def test_no_match_raises(self):
        with pytest.raises(ValidationError, match="Invalid"):
            validate_choice("xyz", ["SINAN", "SIA", "SIM"])

    def test_error_includes_valid_values(self):
        with pytest.raises(ValidationError, match="SINAN, SIA, SIM"):
            validate_choice("bad", ["SINAN", "SIA", "SIM"])

    def test_custom_label(self):
        with pytest.raises(ValidationError, match="Invalid disease"):
            validate_choice("dengue2", ["dengue", "zika"], label="disease")


class TestValidateDataset:
    def test_valid(self):
        assert validate_dataset("sinan") == "SINAN"

    def test_case_insensitive(self):
        assert validate_dataset("SIH") == "SIH"

    def test_typo(self):
        with pytest.raises(ValidationError, match="Did you mean"):
            validate_dataset("sinam")


class TestValidateOrigin:
    def test_valid(self):
        assert validate_origin("ftp") == "FTP"

    def test_typo(self):
        with pytest.raises(ValidationError, match="Did you mean"):
            validate_origin("ftp2")


class TestValidateSource:
    def test_catalog(self):
        assert validate_source("Catalog") == "catalog"

    def test_origin_source(self):
        assert validate_source("ORIGIN") == "origin"

    def test_invalid(self):
        with pytest.raises(ValidationError, match="Invalid source"):
            validate_source("cache")
