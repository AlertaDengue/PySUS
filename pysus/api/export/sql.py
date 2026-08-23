"""SQL DDL export for DataFrames.

Generates CREATE TABLE statements for DuckDB (default), MySQL,
PostgreSQL, and SQLite.

Usage::

    from pysus.api.export.sql import to_sql

    ddl = to_sql(df, "my_table", dialect="duckdb")
    print(ddl)
"""

from __future__ import annotations

from typing import Literal

import pandas as pd


def to_sql(
    df: pd.DataFrame,
    table_name: str,
    dialect: Literal["duckdb", "mysql", "postgresql", "sqlite"] = "duckdb",
    include_data: bool = False,
) -> str:
    """Generate SQL CREATE TABLE statement from DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame (used for schema inference).
    table_name : str
        Name for the SQL table.
    dialect : str
        SQL dialect.
    include_data : bool
        Include INSERT statements.

    Returns
    -------
    str
        SQL DDL string.
    """
    type_map = _get_type_map(dialect)

    columns = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        sql_type = type_map.get(dtype, "TEXT")
        columns.append(f"    {col} {sql_type}")

    create_stmt = (
        f"CREATE TABLE {table_name} (\n" + ",\n".join(columns) + "\n);"
    )

    if include_data:
        inserts = _generate_inserts(df, table_name, dialect)
        return create_stmt + "\n\n" + inserts

    return create_stmt


def _get_type_map(dialect: str) -> dict[str, str]:
    """Get type mapping for SQL dialect."""
    if dialect == "duckdb":
        return {
            "int64": "BIGINT",
            "int32": "INTEGER",
            "int16": "SMALLINT",
            "float64": "DOUBLE",
            "float32": "REAL",
            "object": "VARCHAR",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
            "category": "VARCHAR",
        }
    elif dialect == "mysql":
        return {
            "int64": "BIGINT",
            "int32": "INT",
            "int16": "SMALLINT",
            "float64": "DOUBLE",
            "float32": "FLOAT",
            "object": "TEXT",
            "bool": "TINYINT(1)",
            "datetime64[ns]": "DATETIME",
            "category": "VARCHAR(255)",
        }
    elif dialect == "postgresql":
        return {
            "int64": "BIGINT",
            "int32": "INTEGER",
            "int16": "SMALLINT",
            "float64": "DOUBLE PRECISION",
            "float32": "REAL",
            "object": "TEXT",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
            "category": "TEXT",
        }
    elif dialect == "sqlite":
        return {
            "int64": "INTEGER",
            "int32": "INTEGER",
            "int16": "INTEGER",
            "float64": "REAL",
            "float32": "REAL",
            "object": "TEXT",
            "bool": "INTEGER",
            "datetime64[ns]": "TEXT",
            "category": "TEXT",
        }
    return {}


def _generate_inserts(
    df: pd.DataFrame,
    table_name: str,
    dialect: str,
) -> str:
    """Generate INSERT statements."""
    lines: list[str] = []
    for _, row in df.head(100).iterrows():
        values = []
        for val in row:
            if pd.isna(val):
                values.append("NULL")
            elif isinstance(val, str):
                escaped = val.replace("'", "''")
                values.append(f"'{escaped}'")
            elif isinstance(val, bool):
                values.append("TRUE" if val else "FALSE")
            else:
                values.append(str(val))

        values_str = ", ".join(values)
        lines.append(
            f"INSERT INTO {table_name} VALUES ({values_str});"  # noqa: E702
        )

    return "\n".join(lines)
