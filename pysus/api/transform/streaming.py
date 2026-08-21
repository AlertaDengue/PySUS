"""Large dataset streaming for Parquet files.

Provides chunked iteration to process large files without loading
them entirely into memory.

Usage::

    from pysus.api.transform.streaming import stream_parquet

    for chunk in stream_parquet("data.parquet", chunk_size=10000):
        process(chunk)
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import Any

import pandas as pd


def stream_parquet(
    path: str | Path,
    chunk_size: int = 10000,
    columns: list[str] | None = None,
) -> Generator[pd.DataFrame, None, None]:
    """Stream large Parquet files in chunks.

    Parameters
    ----------
    path : str or Path
        Path to Parquet file.
    chunk_size : int
        Number of rows per chunk.
    columns : list, optional
        Columns to read.

    Yields
    ------
    pd.DataFrame
        DataFrame chunks.
    """
    try:
        import pyarrow.parquet as pq

        parquet_file = pq.ParquetFile(path)
        table = parquet_file.read(columns=columns)

        for i in range(0, len(table), chunk_size):
            yield table.slice(i, chunk_size).to_pandas()

    except ImportError:
        # Fallback: read in chunks using pandas
        df = pd.read_parquet(path, columns=columns)
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i : i + chunk_size]


def stream_with_progress(
    path: str | Path,
    chunk_size: int = 10000,
    callback: Any = None,
) -> Generator[pd.DataFrame, None, None]:
    """Stream with progress callback.

    Parameters
    ----------
    path : str or Path
        Path to Parquet file.
    chunk_size : int
        Rows per chunk.
    callback : callable, optional
        Progress callback function ``(current, total) -> None``.

    Yields
    ------
    pd.DataFrame
        DataFrame chunks.
    """
    try:
        import pyarrow.parquet as pq

        parquet_file = pq.ParquetFile(path)
        total_rows = parquet_file.metadata.num_rows

        for i, batch in enumerate(
            parquet_file.iter_batches(batch_size=chunk_size)
        ):
            if callback:
                callback(min((i + 1) * chunk_size, total_rows), total_rows)
            yield batch.to_pandas()

    except ImportError:
        df = pd.read_parquet(path)
        total = len(df)
        for i in range(0, total, chunk_size):
            if callback:
                callback(min(i + chunk_size, total), total)
            yield df.iloc[i : i + chunk_size]
