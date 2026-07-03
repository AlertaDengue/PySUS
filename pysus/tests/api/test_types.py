import pytest
from pydantic import TypeAdapter, ValidationError
from pysus.api.types import (
    BIGINT,
    BOOLEAN,
    CIHA,
    CNES,
    CSV,
    DADOSGOV,
    DATE,
    DBC,
    DBF,
    DIR,
    DOUBLE,
    DUCKLAKE,
    FILE,
    FLOAT,
    FTP,
    IBGE,
    INTEGER,
    JSON,
    PARQUET,
    PDF,
    PNI,
    SIA,
    SIH,
    SIM,
    SINAN,
    SINASC,
    VARCHAR,
    ZIP,
    ColumnType,
    DatasetName,
    FileType,
    Origin,
    State,
)


class TestOrigin:
    def test_valid_origins(self):
        adapter = TypeAdapter(Origin)
        for origin in (FTP, DADOSGOV, DUCKLAKE):
            assert adapter.validate_python(origin) == origin

    def test_invalid_origin_raises(self):
        with pytest.raises(ValidationError):
            TypeAdapter(Origin).validate_python("INVALID")

    def test_origin_constants(self):
        assert FTP == "FTP"
        assert DADOSGOV == "DadosGov"
        assert DUCKLAKE == "DuckLake"


class TestColumnType:
    def test_valid_column_types(self):
        adapter = TypeAdapter(ColumnType)
        valid = (VARCHAR, INTEGER, BIGINT, FLOAT, DOUBLE, BOOLEAN, DATE)
        for ct in valid:
            assert adapter.validate_python(ct) == ct

    def test_invalid_column_type_raises(self):
        with pytest.raises(ValidationError):
            TypeAdapter(ColumnType).validate_python("INVALID")

    def test_column_type_constants(self):
        assert VARCHAR == "VARCHAR"
        assert INTEGER == "INTEGER"
        assert BIGINT == "BIGINT"
        assert FLOAT == "FLOAT"
        assert DOUBLE == "DOUBLE"
        assert BOOLEAN == "BOOLEAN"
        assert DATE == "DATE"


class TestDatasetName:
    def test_valid_dataset_names(self):
        adapter = TypeAdapter(DatasetName)
        valid = (SINAN, SINASC, SIM, SIH, SIA, PNI, IBGE, CNES, CIHA)
        for dn in valid:
            assert adapter.validate_python(dn) == dn

    def test_invalid_dataset_name_raises(self):
        with pytest.raises(ValidationError):
            TypeAdapter(DatasetName).validate_python("INVALID")

    def test_dataset_name_constants(self):
        assert SINAN == "SINAN"
        assert SINASC == "SINASC"
        assert SIM == "SIM"
        assert SIH == "SIH"
        assert SIA == "SIA"
        assert PNI == "PNI"
        assert IBGE == "IBGE"
        assert CNES == "CNES"
        assert CIHA == "CIHA"


class TestFileType:
    def test_file_types_are_valid(self):
        adapter = TypeAdapter(FileType)
        valid_types = [FILE, DIR, PARQUET, CSV, JSON, PDF, DBC, DBF, ZIP]
        for ft in valid_types:
            assert adapter.validate_python(ft) == ft

    def test_invalid_file_type_raises(self):
        with pytest.raises(ValidationError):
            TypeAdapter(FileType).validate_python("INVALID")


class TestState:
    def test_all_brazilian_states_present(self):
        adapter = TypeAdapter(State)
        expected_states = {
            "AC",
            "AL",
            "AP",
            "AM",
            "BA",
            "CE",
            "ES",
            "GO",
            "MA",
            "MT",
            "MS",
            "MG",
            "PA",
            "PB",
            "PR",
            "PE",
            "PI",
            "RJ",
            "RN",
            "RS",
            "RO",
            "RR",
            "SC",
            "SP",
            "SE",
            "TO",
            "DF",
        }
        for state in expected_states:
            adapter.validate_python(state)

    def test_invalid_state_raises(self):
        with pytest.raises(ValidationError):
            TypeAdapter(State).validate_python("XX")


def test_validate_s3_endpoint_invalid_raises():
    from pysus.api.types import _validate_s3_endpoint

    with pytest.raises(AssertionError):
        _validate_s3_endpoint("invalid-endpoint.com")


def test_validate_s3_region_invalid_raises():
    from pysus.api.types import _validate_s3_region

    with pytest.raises(AssertionError):
        _validate_s3_region("invalid-region")


def test_validate_s3_bucket_invalid_raises():
    from pysus.api.types import _validate_s3_bucket

    with pytest.raises(AssertionError):
        _validate_s3_bucket("invalid-bucket")
