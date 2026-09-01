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
    valid = (FTP, DADOSGOV, DUCKLAKE, SAUDE)
    assert v in valid, f"Invalid origin: {v!r}"
    return v


def _validate_source(v: str) -> str:
    valid = ("catalog", "origin")
    assert v in valid, f"Invalid source: {v!r}"
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
        "JSONL",
        "PDF",
        "DBC",
        "DBF",
        "ZIP",
        "TAR",
        "GZIP",
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
        "ARBOVIROSES",
        "ASSISTENCIASAUDE",
        "ATENCAOPRIMARIA",
        "BNAFAR",
        "CIENCIATECNOLOGIA",
        "DIAGNOSTICOSTRATAMENTOS",
        "ECONOMIASAUDE",
        "EDUCACAOSAUDE",
        "MACROSAUDE",
        "OUVIDORIA",
        "OUTROSTEMAS",
        "PDA",
        "PREVENCAOPROMOCAO",
        "SISAGUA",
        "SISVAN",
        "SAUDEINDIGENA",
        "VACINACAO",
        "VIGILANCIAMEIOAMBIENTE",
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
SAUDE: Annotated[str, AfterValidator(_validate_origin)] = "Saude"

CATALOG: Annotated[str, AfterValidator(_validate_source)] = "catalog"
ORIGIN: Annotated[str, AfterValidator(_validate_source)] = "origin"

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
JSONL: Annotated[str, AfterValidator(_validate_file_type)] = "JSONL"
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
ARBOVIROSES: Annotated[str, AfterValidator(_validate_dataset_name)] = (
    "ARBOVIROSES"
)
ASSISTENCIASAUDE: Annotated[str, AfterValidator(_validate_dataset_name)] = (
    "ASSISTENCIASAUDE"
)
ATENCAOPRIMARIA: Annotated[str, AfterValidator(_validate_dataset_name)] = (
    "ATENCAOPRIMARIA"
)
BNAFAR: Annotated[str, AfterValidator(_validate_dataset_name)] = "BNAFAR"
CIENCIATECNOLOGIA: Annotated[str, AfterValidator(_validate_dataset_name)] = (
    "CIENCIATECNOLOGIA"
)
DIAGNOSTICOSTRATAMENTOS: Annotated[
    str, AfterValidator(_validate_dataset_name)
] = "DIAGNOSTICOSTRATAMENTOS"
ECONOMIASAUDE: Annotated[str, AfterValidator(_validate_dataset_name)] = (
    "ECONOMIASAUDE"
)
EDUCACAOSAUDE: Annotated[str, AfterValidator(_validate_dataset_name)] = (
    "EDUCACAOSAUDE"
)
MACROSAUDE: Annotated[str, AfterValidator(_validate_dataset_name)] = (
    "MACROSAUDE"
)
OUVIDORIA: Annotated[str, AfterValidator(_validate_dataset_name)] = "OUVIDORIA"
OUTROSTEMAS: Annotated[str, AfterValidator(_validate_dataset_name)] = (
    "OUTROSTEMAS"
)
PDA: Annotated[str, AfterValidator(_validate_dataset_name)] = "PDA"
PREVENCAOPROMOCAO: Annotated[str, AfterValidator(_validate_dataset_name)] = (
    "PREVENCAOPROMOCAO"
)
SISAGUA: Annotated[str, AfterValidator(_validate_dataset_name)] = "SISAGUA"
SISVAN: Annotated[str, AfterValidator(_validate_dataset_name)] = "SISVAN"
SAUDEINDIGENA: Annotated[str, AfterValidator(_validate_dataset_name)] = (
    "SAUDEINDIGENA"
)
VACINACAO: Annotated[str, AfterValidator(_validate_dataset_name)] = "VACINACAO"
VIGILANCIAMEIOAMBIENTE: Annotated[
    str, AfterValidator(_validate_dataset_name)
] = "VIGILANCIAMEIOAMBIENTE"

Origin: TypeAlias = Annotated[str, AfterValidator(_validate_origin)]
Source: TypeAlias = Annotated[str, AfterValidator(_validate_source)]
ColumnType: TypeAlias = Annotated[str, AfterValidator(_validate_column_type)]
FileType: TypeAlias = Annotated[str, AfterValidator(_validate_file_type)]
DatasetName: TypeAlias = Annotated[str, AfterValidator(_validate_dataset_name)]
State: TypeAlias = Annotated[str, AfterValidator(_validate_state)]
