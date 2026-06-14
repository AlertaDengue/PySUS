from pysus.api.metadata.models import (
    Column,
    Dataset,
    DatasetGroup,
    File,
    FileMeta,
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


class TestColumnFromSchema:
    def test_from_schema_creates_column(self):
        col = Column.from_schema(
            "ABAND", VARCHAR, description="Abandonment info"
        )
        assert isinstance(col, Column)
        assert col.name == "ABAND"
        assert col.dtype == VARCHAR
        assert col.description == "Abandonment info"

    def test_from_schema_default_description(self):
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
