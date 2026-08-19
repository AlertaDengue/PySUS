"""Tests for pysus.api.saude.schemas YAML column loader."""

from pysus.api.saude.schemas import (
    apply_column_descriptions,
    available_schemas,
    load_endpoint_columns,
)


class TestLoadEndpointColumns:
    def test_load_dengue_schema(self):
        cols = load_endpoint_columns("arboviroses", "dengue")
        assert len(cols) > 0
        names = [c["name"] for c in cols]
        assert "nu_notific" in names
        assert "id_municipio" in names

    def test_load_chikungunya_schema(self):
        cols = load_endpoint_columns("arboviroses", "chikungunya")
        assert len(cols) > 0
        assert cols[0]["name"] == "nu_notific"

    def test_load_zikavirus_schema(self):
        cols = load_endpoint_columns("arboviroses", "zikavirus")
        assert len(cols) > 0

    def test_unknown_dataset_returns_empty(self):
        cols = load_endpoint_columns("nonexistent", "endpoint")
        assert cols == []

    def test_unknown_endpoint_returns_empty(self):
        cols = load_endpoint_columns("arboviroses", "nonexistent")
        assert cols == []

    def test_column_has_required_keys(self):
        cols = load_endpoint_columns("arboviroses", "dengue")
        for col in cols:
            assert "name" in col
            assert "type" in col
            assert "description_pt" in col


class TestAvailableSchemas:
    def test_returns_list(self):
        schemas = available_schemas()
        assert isinstance(schemas, list)

    def test_includes_arboviroses(self):
        schemas = available_schemas()
        assert "arboviroses" in schemas


class TestApplyColumnDescriptions:
    def test_updates_existing_columns(self):
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute(
            """
            CREATE SCHEMA pysus;
            CREATE TABLE pysus.dataset_columns (
                id INTEGER PRIMARY KEY,
                dataset_id INTEGER NOT NULL,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                description VARCHAR,
                nullable BOOLEAN DEFAULT true
            );
        """
        )
        cursor = con.cursor()
        # Insert a column without description
        cursor.execute(
            "INSERT INTO pysus.dataset_columns "
            "(id, dataset_id, name, type, nullable) "
            "VALUES (1, 1, 'nu_notific', 'VARCHAR', true)"
        )
        updated = apply_column_descriptions(
            cursor, dataset_id=1, dataset="arboviroses", endpoint="dengue"
        )
        assert updated >= 1
        cursor.execute(
            "SELECT description FROM pysus.dataset_columns WHERE id = 1"
        )
        row = cursor.fetchone()
        assert row[0] is not None
        assert "notificação" in row[0].lower()
        con.close()

    def test_no_update_for_unknown_dataset(self):
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute(
            """
            CREATE SCHEMA pysus;
            CREATE TABLE pysus.dataset_columns (
                id INTEGER PRIMARY KEY,
                dataset_id INTEGER NOT NULL,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                description VARCHAR,
                nullable BOOLEAN DEFAULT true
            );
        """
        )
        cursor = con.cursor()
        updated = apply_column_descriptions(
            cursor, dataset_id=1, dataset="nonexistent", endpoint="x"
        )
        assert updated == 0
        con.close()
