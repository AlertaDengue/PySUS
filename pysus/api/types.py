from typing import Literal

FileType = Literal[
    None,
    "DIR",
    "PARQUET",
    "CSV",
    "JSON",
    "PDF",
    "DBC",
    "DBF",
    "ZIP",
]
