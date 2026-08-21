"""Tests for pysus.api.metadata.columns module."""

from pysus.api.metadata.columns import (
    available_databases,
    available_groups,
    load_column_metadata,
)


class TestLoadColumnMetadata:
    """Tests for load_column_metadata function."""

    def test_load_sinan_arboviroses(self):
        """Test loading SINAN arboviroses metadata."""
        meta = load_column_metadata("sinan", group="arboviroses")
        assert isinstance(meta, dict)
        assert len(meta) > 0

        # Check that we have some expected columns
        assert "DT_NOTIFIC" in meta or "nu_notific" in [k.upper() for k in meta]

    def test_load_sinan_without_group(self):
        """Test loading SINAN metadata without specifying group."""
        meta = load_column_metadata("sinan")
        assert isinstance(meta, dict)
        assert len(meta) > 0

    def test_load_sihrd_metadata(self):
        """Test loading SIH RD metadata."""
        meta = load_column_metadata("sih", group="rd")
        assert isinstance(meta, dict)
        assert len(meta) > 0

    def test_load_sia_metadata(self):
        """Test loading SIA metadata."""
        meta = load_column_metadata("sia", group="pa")
        assert isinstance(meta, dict)
        assert len(meta) > 0

    def test_load_sim_metadata(self):
        """Test loading SIM metadata."""
        meta = load_column_metadata("sim", group="do")
        assert isinstance(meta, dict)
        assert len(meta) > 0

    def test_load_sinasc_metadata(self):
        """Test loading SINASC metadata."""
        meta = load_column_metadata("sinasc", group="dn")
        assert isinstance(meta, dict)
        assert len(meta) > 0

    def test_column_structure(self):
        """Test that column definitions have expected structure."""
        meta = load_column_metadata("sinan", group="arboviroses")
        for _col_name, col_def in meta.items():
            assert "type" in col_def
            assert "description_pt" in col_def
            assert "description_en" in col_def

    def test_invalid_database_returns_empty(self):
        """Test that invalid database returns empty dict."""
        meta = load_column_metadata("nonexistent_database")
        assert meta == {}

    def test_cache_works(self):
        """Test that second call returns cached result."""
        meta1 = load_column_metadata("sinan", group="arboviroses")
        meta2 = load_column_metadata("sinan", group="arboviroses")
        assert meta1 is meta2  # Same object from cache


class TestAvailableDatabases:
    """Tests for available_databases function."""

    def test_returns_list(self):
        """Test that available_databases returns a list."""
        dbs = available_databases()
        assert isinstance(dbs, list)

    def test_contains_sinan(self):
        """Test that SINAN is always available."""
        dbs = available_databases()
        assert "sinan" in dbs


class TestAvailableGroups:
    """Tests for available_groups function."""

    def test_returns_list(self):
        """Test that available_groups returns a list."""
        groups = available_groups("sinan")
        assert isinstance(groups, list)

    def test_sinan_has_groups(self):
        """Test that SINAN has groups."""
        groups = available_groups("sinan")
        assert len(groups) > 0
