from typing import Annotated, TypeAlias

from pydantic import AfterValidator


def _validate_s3_endpoint(v: str) -> str:
    assert v == "nbg1.your-objectstorage.com"
    return v


def _validate_s3_region(v: str) -> str:
    assert v == "nbg1"
    return v


def _validate_s3_bucket(v: str) -> str:
    assert v == "pysus"
    return v


def _validate_origin(v: str) -> str:
    valid = (FTP, DADOSGOV, DUCKLAKE)
    assert v in valid, f"Invalid origin: {v!r}"
    return v


def _validate_column_type(v: str) -> str:
    valid = (
        "VARCHAR",
        "INTEGER",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "BOOLEAN",
        "DATE",
    )
    assert v in valid, f"Invalid column type: {v!r}"
    return v


def _validate_file_type(v: str) -> str:
    valid = (
        "FILE",
        "DIR",
        "PARQUET",
        "CSV",
        "JSON",
        "PDF",
        "DBC",
        "DBF",
        "ZIP",
    )
    assert v in valid, f"Invalid file type: {v!r}"
    return v


def _validate_dataset_name(v: str) -> str:
    valid = (
        "SINAN",
        "SINASC",
        "SIM",
        "SIH",
        "SIA",
        "PNI",
        "IBGE",
        "CNES",
        "CIHA",
    )
    assert v in valid, f"Invalid dataset name: {v!r}"
    return v


def _validate_state(v: str) -> str:
    valid = (
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
    )
    assert v in valid, f"Invalid state: {v!r}"
    return v


FTP: Annotated[str, AfterValidator(_validate_origin)] = "FTP"
DADOSGOV: Annotated[str, AfterValidator(_validate_origin)] = "DadosGov"
DUCKLAKE: Annotated[str, AfterValidator(_validate_origin)] = "DuckLake"

S3_ENDPOINT: Annotated[str, AfterValidator(_validate_s3_endpoint)] = (
    "nbg1.your-objectstorage.com"
)
S3_REGION: Annotated[str, AfterValidator(_validate_s3_region)] = "nbg1"
S3_BUCKET: Annotated[str, AfterValidator(_validate_s3_bucket)] = "pysus"

VARCHAR: Annotated[str, AfterValidator(_validate_column_type)] = "VARCHAR"
INTEGER: Annotated[str, AfterValidator(_validate_column_type)] = "INTEGER"
BIGINT: Annotated[str, AfterValidator(_validate_column_type)] = "BIGINT"
FLOAT: Annotated[str, AfterValidator(_validate_column_type)] = "FLOAT"
DOUBLE: Annotated[str, AfterValidator(_validate_column_type)] = "DOUBLE"
BOOLEAN: Annotated[str, AfterValidator(_validate_column_type)] = "BOOLEAN"
DATE: Annotated[str, AfterValidator(_validate_column_type)] = "DATE"

FILE: Annotated[str, AfterValidator(_validate_file_type)] = "FILE"
DIR: Annotated[str, AfterValidator(_validate_file_type)] = "DIR"
PARQUET: Annotated[str, AfterValidator(_validate_file_type)] = "PARQUET"
CSV: Annotated[str, AfterValidator(_validate_file_type)] = "CSV"
JSON: Annotated[str, AfterValidator(_validate_file_type)] = "JSON"
PDF: Annotated[str, AfterValidator(_validate_file_type)] = "PDF"
DBC: Annotated[str, AfterValidator(_validate_file_type)] = "DBC"
DBF: Annotated[str, AfterValidator(_validate_file_type)] = "DBF"
ZIP: Annotated[str, AfterValidator(_validate_file_type)] = "ZIP"

SINAN: Annotated[str, AfterValidator(_validate_dataset_name)] = "SINAN"
SINASC: Annotated[str, AfterValidator(_validate_dataset_name)] = "SINASC"
SIM: Annotated[str, AfterValidator(_validate_dataset_name)] = "SIM"
SIH: Annotated[str, AfterValidator(_validate_dataset_name)] = "SIH"
SIA: Annotated[str, AfterValidator(_validate_dataset_name)] = "SIA"
PNI: Annotated[str, AfterValidator(_validate_dataset_name)] = "PNI"
IBGE: Annotated[str, AfterValidator(_validate_dataset_name)] = "IBGE"
CNES: Annotated[str, AfterValidator(_validate_dataset_name)] = "CNES"
CIHA: Annotated[str, AfterValidator(_validate_dataset_name)] = "CIHA"

Origin: TypeAlias = Annotated[str, AfterValidator(_validate_origin)]
ColumnType: TypeAlias = Annotated[str, AfterValidator(_validate_column_type)]
FileType: TypeAlias = Annotated[str, AfterValidator(_validate_file_type)]
DatasetName: TypeAlias = Annotated[str, AfterValidator(_validate_dataset_name)]
State: TypeAlias = Annotated[str, AfterValidator(_validate_state)]
