"""Tests for pysus.api.metadata.versioning module."""

from pysus.api.metadata.versioning import (
    detect_schema_change,
    get_schema_version,
    list_schema_versions,
    schema_fingerprint,
)


class TestGetSchemaVersion:
    def test_returns_string(self):
        version = get_schema_version("sinan", 2024)
        assert isinstance(version, str)
        assert "sinan" in version

    def test_consistent(self):
        v1 = get_schema_version("sinan", 2024)
        v2 = get_schema_version("sinan", 2024)
        assert v1 == v2


class TestListSchemaVersions:
    def test_returns_dict(self):
        versions = list_schema_versions("sinan")
        assert isinstance(versions, dict)

    def test_keys_are_ints(self):
        versions = list_schema_versions("sinan")
        for key in versions:
            assert isinstance(key, int)


class TestDetectSchemaChange:
    def test_same_year_no_changes(self):
        changes = detect_schema_change("sinan", 2024, 2024)
        assert changes["added"] == []
        assert changes["removed"] == []
        assert changes["changed"] == {}

    def test_returns_expected_keys(self):
        changes = detect_schema_change("sinan", 2023, 2024)
        assert "added" in changes
        assert "removed" in changes
        assert "changed" in changes
        assert "version1" in changes
        assert "version2" in changes

    def test_lists_are_sorted(self):
        changes = detect_schema_change("sinan", 2023, 2024)
        assert changes["added"] == sorted(changes["added"])
        assert changes["removed"] == sorted(changes["removed"])


class TestSchemaFingerprint:
    def test_returns_hex_string(self):
        fp = schema_fingerprint("sinan")
        assert isinstance(fp, str)
        assert len(fp) == 32
        assert all(c in "0123456789abcdef" for c in fp)

    def test_consistent(self):
        fp1 = schema_fingerprint("sinan")
        fp2 = schema_fingerprint("sinan")
        assert fp1 == fp2

    def test_different_database_different_fingerprint(self):
        fp1 = schema_fingerprint("sinan")
        fp2 = schema_fingerprint("sih")
        assert fp1 != fp2
