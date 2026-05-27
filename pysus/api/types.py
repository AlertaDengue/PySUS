"""Type aliases used across the PySUS API.

FileType:
    Discriminated union of supported local file types
    (FILE, DIR, PARQUET, CSV, JSON, PDF, DBC, DBF, ZIP).

State:
    Brazilian state abbreviations (AC, AL, AP, ..., DF).
"""

from typing import Final, Literal

FTP: Final = "FTP"
DadosGov: Final = "DadosGov"
DuckLake: Final = "DuckLake"

Origin = Literal[FTP, DadosGov, DuckLake]

VARCHAR: Final = "VARCHAR"
INTEGER: Final = "INTEGER"
BIGINT: Final = "BIGINT"
FLOAT: Final = "FLOAT"
DOUBLE: Final = "DOUBLE"
BOOLEAN: Final = "BOOLEAN"
DATE: Final = "DATE"

ColumnType = Literal[VARCHAR, INTEGER, BIGINT, FLOAT, DOUBLE, BOOLEAN, DATE]

FILE: Final = "FILE"
DIR: Final = "DIR"
PARQUET: Final = "PARQUET"
CSV: Final = "CSV"
JSON: Final = "JSON"
PDF: Final = "PDF"
DBC: Final = "DBC"
DBF: Final = "DBF"
ZIP: Final = "ZIP"

FileType = Literal[FILE, DIR, PARQUET, CSV, JSON, PDF, DBC, DBF, ZIP]

SINAN: Final = "SINAN"
SINASC: Final = "SINASC"
SIM: Final = "SIM"
SIH: Final = "SIH"
SIA: Final = "SIA"
PNI: Final = "PNI"
IBGE: Final = "IBGE"
CNES: Final = "CNES"
CIHA: Final = "CIHA"

DatasetName = Literal[SINAN, SINASC, SIM, SIH, SIA, PNI, IBGE, CNES, CIHA]

State = Literal[
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
]
