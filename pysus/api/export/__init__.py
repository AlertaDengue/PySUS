"""Export utilities for PySUS DataFrames.

Provides CSV/Excel, GeoJSON, SQL DDL, and auto-detecting ``export()``.

Usage::

    from pysus.api.export import export, to_csv, to_excel, to_geojson, to_sql

    export(df, "output.csv")
    export(df, "output.parquet")
    export(df, "output.xlsx", sheet_name="Data")
    to_geojson(df, "output.geojson", lat_col="LAT", lon_col="LON")
    ddl = to_sql(df, "table_name", dialect="duckdb")
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from pysus.api.export.csv_excel import to_csv, to_excel
from pysus.api.export.geojson import to_geojson
from pysus.api.export.sql import to_sql


def export(
    df: pd.DataFrame,
    path: str | Path,
    **kwargs: object,
) -> Path:
    """Export DataFrame to a file, auto-detecting format from extension.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    path : str or Path
        Output file path.
    **kwargs
        Additional arguments passed to the format-specific writer.

    Returns
    -------
    Path
        Path to created file.

    Raises
    ------
    ValueError
        If the file extension is not supported.

    Supported Formats
    -----------------
    - ``.csv`` — CSV with metadata sidecar
    - ``.xlsx`` — Excel with metadata sheet
    - ``.parquet`` — Parquet (native pandas)
    - ``.json`` — JSON lines
    - ``.sqlite`` — SQLite table (requires ``table_name`` kwarg)
    """
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix == ".csv":
        return to_csv(df, path, **kwargs)  # type: ignore[arg-type,return-value]
    elif suffix == ".xlsx":
        return to_excel(  # type: ignore[arg-type,return-value]
            df, path, **kwargs  # type: ignore[arg-type]
        )
    elif suffix == ".parquet":
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, **kwargs)  # type: ignore[call-overload]
        return path
    elif suffix == ".json":
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_json(  # type: ignore[call-overload]
            path,
            orient="records",
            lines=True,
            **kwargs,
        )
        return path
    elif suffix == ".sqlite":
        table_name = kwargs.pop("table_name", None)
        if table_name is None:
            table_name = path.stem
        import sqlite3

        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path))
        df.to_sql(  # type: ignore[arg-type,call-overload]
            str(table_name),
            conn,
            if_exists="replace",
            index=False,
        )
        conn.close()
        return path
    else:
        raise ValueError(
            f"Unsupported format: {suffix}. "
            "Supported: .csv, .xlsx, .parquet, .json, .sqlite"
        )


__all__ = [
    "export",
    "to_csv",
    "to_excel",
    "to_geojson",
    "to_sql",
]
