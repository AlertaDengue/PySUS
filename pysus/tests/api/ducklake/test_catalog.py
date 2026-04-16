from pysus.api.ducklake.catalog import (
    CatalogDataset,
    CatalogFile,
    CatalogTable,
    ColumnDefinition,
    DatasetGroup,
    Origin,
    file_columns,
)


class TestOrigin:
    def test_origin_ftp(self):
        assert Origin.FTP.value == "ftp"

    def test_origin_api(self):
        assert Origin.API.value == "api"


class TestCatalogTable:
    def test_catalog_table_is_abstract(self):
        assert CatalogTable.__abstract__ is True


class TestCatalogDataset:
    def test_catalog_dataset_tablename(self):
        assert CatalogDataset.__tablename__ == "datasets"

    def test_catalog_dataset_columns(self):
        assert "id" in CatalogDataset.__table__.columns
        assert "name" in CatalogDataset.__table__.columns
        assert "long_name" in CatalogDataset.__table__.columns
        assert "origin" in CatalogDataset.__table__.columns


class TestColumnDefinition:
    def test_column_definition_tablename(self):
        assert ColumnDefinition.__tablename__ == "dataset_columns"

    def test_column_definition_columns(self):
        assert "id" in ColumnDefinition.__table__.columns
        assert "dataset_id" in ColumnDefinition.__table__.columns
        assert "name" in ColumnDefinition.__table__.columns
        assert "type" in ColumnDefinition.__table__.columns


class TestDatasetGroup:
    def test_dataset_group_tablename(self):
        assert DatasetGroup.__tablename__ == "dataset_groups"

    def test_dataset_group_columns(self):
        assert "id" in DatasetGroup.__table__.columns
        assert "dataset_id" in DatasetGroup.__table__.columns
        assert "name" in DatasetGroup.__table__.columns
        assert "long_name" in DatasetGroup.__table__.columns


class TestCatalogFile:
    def test_catalog_file_tablename(self):
        assert CatalogFile.__tablename__ == "files"

    def test_catalog_file_columns(self):
        assert "id" in CatalogFile.__table__.columns
        assert "dataset_id" in CatalogFile.__table__.columns
        assert "path" in CatalogFile.__table__.columns
        assert "size" in CatalogFile.__table__.columns
        assert "rows" in CatalogFile.__table__.columns
        assert "modified" in CatalogFile.__table__.columns
        assert "year" in CatalogFile.__table__.columns
        assert "month" in CatalogFile.__table__.columns
        assert "state" in CatalogFile.__table__.columns


class TestFileColumns:
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
