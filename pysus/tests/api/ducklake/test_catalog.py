"""Tests for DuckLake catalog ORM models."""

from pysus.api.ducklake.catalog.orm.dataset import (
    ColumnDefinition,
    Dataset,
    File,
    Group,
    file_columns,
)
from pysus.api.ducklake.catalog.orm.default import Dataset as DefaultDataset


class TestDefaultDataset:
    def test_tablename(self):
        assert DefaultDataset.__tablename__ == "datasets"

    def test_columns(self):
        cols = DefaultDataset.__table__.columns
        assert "id" in cols
        assert "name" in cols
        assert "long_name" in cols
        assert "description" in cols

    def test_schema(self):
        assert DefaultDataset.__table_args__[0]["schema"] == "pysus"


class TestDataset:
    def test_tablename(self):
        assert Dataset.__tablename__ == "datasets"

    def test_columns(self):
        cols = Dataset.__table__.columns
        assert "id" in cols
        assert "name" in cols
        assert "long_name" in cols
        assert "description" in cols

    def test_schema(self):
        assert Dataset.__table_args__[0]["schema"] == "pysus"

    def test_relationships(self):
        assert hasattr(Dataset, "groups")
        assert hasattr(Dataset, "files")
        assert hasattr(Dataset, "columns")


class TestColumnDefinition:
    def test_tablename(self):
        assert ColumnDefinition.__tablename__ == "dataset_columns"

    def test_columns(self):
        cols = ColumnDefinition.__table__.columns
        assert "id" in cols
        assert "dataset_id" in cols
        assert "name" in cols
        assert "type" in cols
        assert "description" in cols
        assert "nullable" in cols


class TestGroup:
    def test_tablename(self):
        assert Group.__tablename__ == "dataset_groups"

    def test_columns(self):
        cols = Group.__table__.columns
        assert "id" in cols
        assert "dataset_id" in cols
        assert "name" in cols
        assert "long_name" in cols
        assert "description" in cols

    def test_relationships(self):
        assert hasattr(Group, "dataset")
        assert hasattr(Group, "files")


class TestFile:
    def test_tablename(self):
        assert File.__tablename__ == "files"

    def test_columns(self):
        cols = File.__table__.columns
        assert "id" in cols
        assert "dataset_id" in cols
        assert "group_id" in cols
        assert "path" in cols
        assert "size" in cols
        assert "rows" in cols
        assert "type" in cols
        assert "modified" in cols
        assert "year" in cols
        assert "month" in cols
        assert "state" in cols
        assert "sha256" in cols
        assert "origin_size" in cols
        assert "origin_path" in cols

    def test_relationships(self):
        assert hasattr(File, "dataset")
        assert hasattr(File, "group")
        assert hasattr(File, "columns")


class TestFileColumns:
    def test_file_columns_table_name(self):
        assert file_columns.name == "file_columns"

    def test_file_columns_primary_keys(self):
        file_id_col = file_columns.c.file_id
        column_id_col = file_columns.c.column_id
        assert file_id_col.primary_key is True
        assert column_id_col.primary_key is True

    def test_file_columns_foreign_keys(self):
        file_id_col = file_columns.c.file_id
        column_id_col = file_columns.c.column_id
        assert file_id_col.foreign_keys
        assert column_id_col.foreign_keys
