import duckdb
from pathlib import Path


def csv_to_parquet(csv_file: Path) -> Path:
    parquet = csv_file.with_suffix(".parquet")
    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT *
            FROM read_csv_auto('{csv_file}')
        )
        TO '{parquet}'
        (FORMAT PARQUET)
    """)
    return parquet
