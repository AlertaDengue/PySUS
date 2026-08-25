"""Tests for pysus.api.columns module (Phase 1.3)."""

from pysus.api.columns import ColumnInfo, search_columns


class TestColumnInfo:
    def test_creation(self):
        col = ColumnInfo(
            name="dt_notific",
            description="Data da notificação",
            description_en="Notification date",
            dtype="string",
            dataset="arboviroses",
            endpoint="dengue",
        )
        assert col.name == "dt_notific"
        assert col.description_en == "Notification date"

    def test_defaults(self):
        col = ColumnInfo(name="test")
        assert col.description == ""
        assert col.dtype == ""
        assert col.dataset == ""
        assert col.endpoint == ""
        assert col.categories == ""
        assert col.characteristics == ""
        assert col.required is False

    def test_full_metadata_fields(self):
        col = ColumnInfo(
            name="con_classi",
            categories="1. Forma bubônica 2. Forma pneumônica",
            characteristics="Campo Essencial",
            required=True,
        )
        assert "bubônica" in col.categories
        assert col.characteristics == "Campo Essencial"
        assert col.required is True

    def test_frozen(self):
        col = ColumnInfo(name="test")
        try:
            col.name = "other"
        except AttributeError:
            return
        raise AssertionError("Should be frozen")


class TestSearchColumns:
    def test_search_all_arboviroses(self):
        results = search_columns("arboviroses")
        assert len(results) > 0
        names = {c.name for c in results}
        assert "dt_notific" in names
        assert "cs_sexo" in names

    def test_search_by_query(self):
        results = search_columns("arboviroses", "notification")
        assert len(results) > 0
        for col in results:
            assert (
                "notification" in col.description_en.lower()
                or "notification" in col.name.lower()
            )

    def test_search_by_endpoint(self):
        results = search_columns("arboviroses", endpoint="dengue")
        assert len(results) > 0
        for col in results:
            assert col.endpoint == "dengue"

    def test_search_sinan_typecast(self):
        results = search_columns("sinan")
        assert len(results) > 0
        names = {c.name for c in results}
        assert "dt_notific" in names

    def test_search_sinan_endpoint_metadata(self):
        """SINAN disease forms expose full metadata (categories etc.)."""
        results = search_columns("sinan", endpoint="peste")
        con_classi = next(c for c in results if c.name == "con_classi")
        assert "bubônica" in con_classi.categories
        assert "confirmado" in con_classi.characteristics
        assert con_classi.endpoint == "peste"

    def test_search_query_matches_categories(self):
        results = search_columns("sinan", "bubônica")
        assert any(c.name == "con_classi" for c in results)

    def test_search_no_results(self):
        results = search_columns("arboviroses", "xyz_nonexistent")
        assert results == []

    def test_search_empty_dataset(self):
        results = search_columns("nonexistent_dataset")
        # May return empty or only typecast results
        assert isinstance(results, list)

    def test_search_returns_sorted(self):
        results = search_columns("arboviroses")
        names = [c.name for c in results]
        assert names == sorted(names)

    def test_top_level_export(self):
        import pysus

        assert hasattr(pysus, "search_columns")
        assert hasattr(pysus, "ColumnInfo")


class TestSearchColumnsDedup:
    def test_deduplicates_across_sources(self):
        """Column from both YAML and typecast is deduplicated."""
        results = search_columns("sinan")
        names = [c.name for c in results]
        # No duplicate names
        assert len(names) == len(set(names))
