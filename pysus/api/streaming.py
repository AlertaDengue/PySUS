"""Streaming DataFrame utilities using DuckDB.

Provides ``query_parquet()`` that returns a DuckDB relation for
lazy evaluation on large Parquet datasets.

Usage::

    from pysus.api.streaming import query_parquet, to_df, to_arrow

    rel = query_parquet("path/to/files/*.parquet")
    df = to_df(rel)           # materialize to DataFrame
    table = to_arrow(rel)     # export to Arrow
"""

from __future__ import annotations

from pathlib import Path

import duckdb


def query_parquet(
    path: str | Path,
    query: str = "SELECT * FROM data",
) -> duckdb.DuckDBPyRelation:
    """Query Parquet files as a DuckDB relation.

    Parameters
    ----------
    path : str or Path
        Path to a single Parquet file or glob pattern.
    query : str
        SQL query. The table name ``data`` refers to the Parquet file(s).

    Returns
    -------
    duckdb.DuckDBPyRelation
        Lazy relation for further processing.

    Examples
    --------
    >>> rel = query_parquet("data.parquet")
    >>> rel = query_parquet("data.parquet", "SELECT COUNT(*) FROM data")
    """
    path = Path(path)
    conn = duckdb.connect()

    if "*" in str(path) or path.is_dir():
        glob_path = str(path) if "*" in str(path) else f"{path}/*.parquet"
        conn.execute(
            f"CREATE VIEW data AS SELECT * FROM read_parquet('{glob_path}')"
        )
    else:
        conn.execute(
            f"CREATE VIEW data AS SELECT * FROM read_parquet('{path}')"
        )

    return conn.sql(query)


def to_df(rel: duckdb.DuckDBPyRelation):
    """Materialize a DuckDB relation to a pandas DataFrame.

    Parameters
    ----------
    rel : duckdb.DuckDBPyRelation
        The relation to materialize.

    Returns
    -------
    pd.DataFrame
        The result as a DataFrame.
    """
    return rel.to_df()


def to_arrow(rel: duckdb.DuckDBPyRelation):
    """Export a DuckDB relation to an Arrow table.

    Parameters
    ----------
    rel : duckdb.DuckDBPyRelation
        The relation to export.

    Returns
    -------
    pyarrow.Table
        The result as an Arrow table.
    """
    return rel.to_arrow_table()
