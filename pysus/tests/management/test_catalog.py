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
