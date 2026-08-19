"""Tests for pysus.management.catalog CatalogWriter DB operations."""

from datetime import datetime
from unittest.mock import MagicMock

import duckdb
import pytest
from pysus.management.catalog import CatalogWriter

_SCHEMA = """
CREATE SCHEMA pysus;
CREATE TABLE pysus.files (
    id INTEGER,
    dataset_id INTEGER,
    group_id INTEGER,
    path VARCHAR UNIQUE,
    size BIGINT,
    rows INTEGER,
    type VARCHAR,
    modified TIMESTAMP,
    origin_modified TIMESTAMP,
    origin_size BIGINT,
    origin_path VARCHAR,
    sha256 VARCHAR,
    source_sha256 VARCHAR,
    origin VARCHAR,
    format VARCHAR,
    year INTEGER,
    month INTEGER,
    state VARCHAR
);
"""

_MINIMAL_SCHEMA = """
CREATE SCHEMA pysus;
CREATE TABLE pysus.files (
    id INTEGER,
    dataset_id INTEGER,
    group_id INTEGER,
    path VARCHAR UNIQUE,
    size BIGINT,
    rows INTEGER,
    type VARCHAR,
    modified TIMESTAMP,
    origin_modified TIMESTAMP,
    origin_size BIGINT,
    origin_path VARCHAR,
    sha256 VARCHAR,
    year INTEGER,
    month INTEGER,
    state VARCHAR
);
"""


@pytest.fixture
def writer_and_cursor():
    writer = CatalogWriter(ducklake=MagicMock())
    con = duckdb.connect(":memory:")
    con.execute(_SCHEMA)
    return writer, con.cursor(), con


def _insert(
    cursor,
    path,
    origin_modified=None,
    origin_size=0,
    sha256=None,
    source_sha256=None,
):
    cursor.execute(
        "INSERT INTO pysus.files (id, dataset_id, group_id, path, size, "
        "rows, modified, origin_modified, origin_size, origin_path, "
        "sha256, source_sha256, year, month, state) "
        "VALUES (?, ?, ?, ?, 0, 0, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, "
        "NULL, NULL, NULL)",
        (
            1,
            8,
            None,
            path,
            origin_modified,
            origin_size,
            "ftp/x",
            sha256,
            source_sha256,
        ),
    )


class TestGetFileFull:
    def test_missing(self, writer_and_cursor):
        writer, cursor, _ = writer_and_cursor
        assert writer.get_file_full(cursor, "public/data/x.parquet") is None

    def test_full_row(self, writer_and_cursor):
        writer, cursor, _ = writer_and_cursor
        _insert(
            cursor,
            "public/data/x.parquet",
            origin_modified=datetime(2026, 1, 1),
            origin_size=42,
            sha256="aa" * 32,
            source_sha256="bb" * 32,
        )
        result = writer.get_file_full(cursor, "public/data/x.parquet")
        assert result is not None
        file_id, modified, size, sha, source = result
        assert file_id == 1
        assert modified == datetime(2026, 1, 1)
        assert size == 42
        assert sha == "aa" * 32
        assert source == "bb" * 32


class TestTouchFile:
    def test_updates_origin_metadata(self, writer_and_cursor):
        writer, cursor, _ = writer_and_cursor
        _insert(cursor, "public/data/x.parquet")
        writer.touch_file(
            cursor, 1, datetime(2026, 2, 2), 99, source_sha256="cc" * 32
        )
        result = writer.get_file_full(cursor, "public/data/x.parquet")
        assert result is not None
        assert result[1] == datetime(2026, 2, 2)
        assert result[2] == 99
        assert result[4] == "cc" * 32
        assert result[3] is None

    def test_without_source_sha256(self, writer_and_cursor):
        writer, cursor, _ = writer_and_cursor
        _insert(cursor, "public/data/x.parquet")
        writer.touch_file(cursor, 1, datetime(2026, 2, 2), 99)
        result = writer.get_file_full(cursor, "public/data/x.parquet")
        assert result is not None
        assert result[1] == datetime(2026, 2, 2)
        assert result[2] == 99


class TestUpsertFile:
    def test_insert_stores_hashes(self, writer_and_cursor):
        writer, cursor, _ = writer_and_cursor
        file_id, created = writer.upsert_file(
            cursor,
            dataset_id=8,
            group_id=None,
            path="public/data/x.parquet",
            size=10,
            rows=3,
            modified=datetime(2026, 1, 1),
            origin_modified=datetime(2026, 1, 1),
            origin_size=11,
            origin_path="ftp/x",
            year=2026,
            month=None,
            state=None,
            origin="ftp",
            format="parquet",
            sha256="aa" * 32,
            source_sha256="bb" * 32,
        )
        assert created is True
        result = writer.get_file_full(cursor, "public/data/x.parquet")
        assert result is not None
        assert result[0] == file_id
        assert result[3] == "aa" * 32
        assert result[4] == "bb" * 32

    def test_update_stores_hashes(self, writer_and_cursor):
        writer, cursor, _ = writer_and_cursor
        _insert(cursor, "public/data/x.parquet")
        file_id, created = writer.upsert_file(
            cursor,
            dataset_id=8,
            group_id=None,
            path="public/data/x.parquet",
            size=99,
            rows=9,
            modified=datetime(2026, 1, 1),
            origin_modified=datetime(2026, 1, 1),
            origin_size=11,
            origin_path="ftp/x",
            year=None,
            month=None,
            state=None,
            sha256="aa" * 32,
            source_sha256="bb" * 32,
        )
        assert created is False
        assert file_id == 1
        result = writer.get_file_full(cursor, "public/data/x.parquet")
        assert result is not None
        assert result[2] == 11
        assert result[3] == "aa" * 32
        assert result[4] == "bb" * 32


class TestEnsureManagementColumns:
    def test_adds_source_sha256_column(self):
        writer = CatalogWriter(ducklake=MagicMock())
        con = duckdb.connect(":memory:")
        con.execute(_MINIMAL_SCHEMA)
        cursor = con.cursor()
        writer._ensure_management_columns(cursor)
        cursor.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'pysus' AND table_name = 'files' "
            "AND column_name IN ('origin', 'format', 'source_sha256')"
        )
        assert {row[0] for row in cursor.fetchall()} == {
            "origin",
            "format",
            "source_sha256",
        }


_CENTRAL_SCHEMA = """
CREATE SCHEMA pysus;
CREATE TABLE pysus.datasets (
    id INTEGER PRIMARY KEY,
    name VARCHAR UNIQUE NOT NULL,
    long_name VARCHAR NOT NULL,
    description VARCHAR
);
"""

_FULL_SCHEMA = (
    _CENTRAL_SCHEMA
    + """
CREATE TABLE pysus.files (
    id INTEGER,
    dataset_id INTEGER,
    group_id INTEGER,
    path VARCHAR UNIQUE,
    size BIGINT,
    rows INTEGER,
    type VARCHAR,
    modified TIMESTAMP,
    origin_modified TIMESTAMP,
    origin_size BIGINT,
    origin_path VARCHAR,
    sha256 VARCHAR,
    source_sha256 VARCHAR,
    origin VARCHAR,
    format VARCHAR,
    year INTEGER,
    month INTEGER,
    state VARCHAR
);
"""
)


@pytest.fixture
def saude_catalog():
    writer = CatalogWriter(ducklake=MagicMock())
    con = duckdb.connect(":memory:")
    con.execute(_FULL_SCHEMA)
    return writer, con.cursor(), con


class TestSaudeDatasetCatalog:
    def test_ensure_dataset_creates_saude_entry(self, saude_catalog):
        writer, cursor, _ = saude_catalog
        ds_id = writer.ensure_dataset(
            cursor,
            name="ARBOVIROSES_DENGUE",
            long_name="Arboviroses - Dengue",
            description="Dados abertos sobre dengue",
        )
        assert ds_id == 1
        cursor.execute(
            "SELECT name, long_name FROM pysus.datasets WHERE id = 1"
        )
        row = cursor.fetchone()
        assert row[0] == "arboviroses_dengue"
        assert row[1] == "Arboviroses - Dengue"

    def test_upsert_file_with_state(self, saude_catalog):
        writer, cursor, _ = saude_catalog
        ds_id = writer.ensure_dataset(
            cursor,
            name="ARBOVIROSES_DENGUE",
            long_name="Arboviroses - Dengue",
        )
        file_id, created = writer.upsert_file(
            cursor,
            dataset_id=ds_id,
            group_id=None,
            path="public/data/saude/arboviroses_dengue/_/2024/05/SP/br.parquet",
            size=1024,
            rows=100,
            modified=datetime(2026, 1, 1),
            origin_modified=datetime(2026, 1, 1),
            origin_size=1024,
            origin_path="https://apidadosabertos.saude.gov.br/api/v1/dengue",
            year=2024,
            month=5,
            state="SP",
            origin="saude",
            format="parquet",
        )
        assert created is True
        cursor.execute(
            "SELECT state, origin FROM pysus.files WHERE id = ?", (file_id,)
        )
        row = cursor.fetchone()
        assert row[0] == "SP"
        assert row[1] == "saude"

    def test_upsert_file_national_no_state(self, saude_catalog):
        writer, cursor, _ = saude_catalog
        ds_id = writer.ensure_dataset(
            cursor,
            name="ARBOVIROSES_DENGUE",
            long_name="Arboviroses - Dengue",
        )
        file_id, created = writer.upsert_file(
            cursor,
            dataset_id=ds_id,
            group_id=None,
            path="public/data/saude/arboviroses_dengue/_/2024/_/BR/br.parquet",
            size=2048,
            rows=200,
            modified=datetime(2026, 1, 1),
            origin_modified=datetime(2026, 1, 1),
            origin_size=2048,
            origin_path="https://apidadosabertos.saude.gov.br/api/v1/dengue",
            year=2024,
            month=None,
            state=None,
            origin="saude",
            format="parquet",
        )
        assert created is True
        cursor.execute(
            "SELECT state, origin FROM pysus.files WHERE id = ?", (file_id,)
        )
        row = cursor.fetchone()
        assert row[0] is None
        assert row[1] == "saude"

    def test_query_includes_null_state_files(self, saude_catalog):
        writer, cursor, con = saude_catalog
        ds_id = writer.ensure_dataset(
            cursor,
            name="ARBOVIROSES_DENGUE",
            long_name="Arboviroses - Dengue",
        )
        # Insert one file with state and one without
        writer.upsert_file(
            cursor,
            dataset_id=ds_id,
            group_id=None,
            path="public/data/saude/arr/2024/05/SP/dengue_sp.parquet",
            size=100,
            rows=10,
            modified=datetime(2026, 1, 1),
            origin_modified=datetime(2026, 1, 1),
            origin_size=100,
            origin_path="x",
            year=2024,
            month=5,
            state="SP",
        )
        writer.upsert_file(
            cursor,
            dataset_id=ds_id,
            group_id=None,
            path="public/data/saude/arr/2024/_/BR/dengue_br.parquet",
            size=200,
            rows=20,
            modified=datetime(2026, 1, 1),
            origin_modified=datetime(2026, 1, 1),
            origin_size=200,
            origin_path="y",
            year=2024,
            month=None,
            state=None,
        )
        # Query with state filter — should include both SP and NULL-state
        cursor.execute(
            "SELECT path, state FROM pysus.files "
            "WHERE dataset_id = ? AND "
            "(state IN ('SP') OR state IS NULL) "
            "ORDER BY path",
            (ds_id,),
        )
        rows = cursor.fetchall()
        assert len(rows) == 2
        states = {r[1] for r in rows}
        assert None in states
        assert "SP" in states

    def test_multiple_saude_datasets(self, saude_catalog):
        writer, cursor, _ = saude_catalog
        id1 = writer.ensure_dataset(
            cursor, name="ARBOVIROSES_DENGUE", long_name="Dengue"
        )
        id2 = writer.ensure_dataset(
            cursor, name="SISAGUA", long_name="Sistema de Agua"
        )
        assert id1 != id2
        cursor.execute("SELECT COUNT(*) FROM pysus.datasets")
        assert cursor.fetchone()[0] == 2

    def test_ensure_dataset_idempotent(self, saude_catalog):
        writer, cursor, _ = saude_catalog
        id1 = writer.ensure_dataset(
            cursor, name="ARBOVIROSES_DENGUE", long_name="Dengue v1"
        )
        id2 = writer.ensure_dataset(
            cursor, name="ARBOVIROSES_DENGUE", long_name="Dengue v2"
        )
        assert id1 == id2
        cursor.execute(
            "SELECT long_name FROM pysus.datasets WHERE id = ?", (id1,)
        )
        assert cursor.fetchone()[0] == "Dengue v2"
