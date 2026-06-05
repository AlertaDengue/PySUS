import builtins
from unittest.mock import patch

from pysus.api.metadata.models import (
    Column,
    Dataset,
    DatasetGroup,
    File,
    FileMeta,
    lookup_column_meta,
    pick_description,
)
from pysus.api.metadata.report import Columns, Footer, Header
from pysus.api.types import VARCHAR


class TestReportClasses:
    def test_header_instantiation(self):
        h = Header()
        assert isinstance(h, Header)

    def test_columns_instantiation(self):
        c = Columns()
        assert isinstance(c, Columns)

    def test_footer_instantiation(self):
        f = Footer()
        assert isinstance(f, Footer)


class TestLookupColumnMeta:
    def test_found_returns_dict(self):
        meta = lookup_column_meta("ABAND")
        assert meta is not None
        assert isinstance(meta, dict)

    def test_not_found_returns_none(self):
        meta = lookup_column_meta("NONEXISTENT_COLUMN_XYZ")
        assert meta is None

    def test_import_error_returns_none(self):
        with patch.object(
            builtins, "__import__", side_effect=ImportError("mock")
        ):
            result = lookup_column_meta("ABAND")
            assert result is None


class TestPickDescription:
    def test_none_meta_returns_empty(self):
        assert pick_description(None) == ""

    def test_non_empty_value_returns_first_value(self):
        meta = {"sinan": "Some description"}
        assert pick_description(meta) == "Some description"

    def test_empty_dict_returns_empty(self):
        assert pick_description({}) == ""

    def test_all_empty_values_returns_empty(self):
        meta = {"sinan": "", "sih": ""}
        assert pick_description(meta) == ""


class TestColumnFromSchema:
    def test_from_schema_creates_column(self):
        col = Column.from_schema("ABAND", VARCHAR)
        assert isinstance(col, Column)
        assert col.name == "ABAND"
        assert col.dtype == VARCHAR

    def test_from_schema_unknown_column(self):
        col = Column.from_schema("NONEXISTENT_COLUMN_XYZ", VARCHAR)
        assert col.name == "NONEXISTENT_COLUMN_XYZ"
        assert col.description == ""
        assert col.dtype == VARCHAR


class TestDataclassInstantiations:
    def test_dataset(self):
        d = Dataset(name="sinan", long_name="SINAN", description="Test")
        assert d.name == "sinan"
        assert d.long_name == "SINAN"
        assert d.description == "Test"

    def test_dataset_group(self):
        dg = DatasetGroup(name="sinan", long_name="SINAN", description="Test")
        assert dg.name == "sinan"
        assert dg.long_name == "SINAN"
        assert dg.description == "Test"

    def test_file_meta(self):
        fm = FileMeta(name="test", path="/tmp", size=100)
        assert fm.name == "test"
        assert fm.path == "/tmp"
        assert fm.size == 100

    def test_file(self):
        f = File(origin="FTP")
        assert f.origin == "FTP"
        assert f.dataset is None
        assert f.group is None
        assert f.columns == []

    def test_column(self):
        col = Column(name="ABAND", description="Test", dtype=VARCHAR)
        assert col.name == "ABAND"
        assert col.description == "Test"
        assert col.dtype == VARCHAR
