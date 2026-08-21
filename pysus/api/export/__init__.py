"""Export utilities for PySUS DataFrames.

Provides CSV/Excel, GeoJSON, and SQL DDL export.

Usage::

    from pysus.api.export import to_csv, to_excel, to_geojson, to_sql

    to_csv(df, "output.csv", metadata={"source": "DATASUS"})
    to_excel(df, "output.xlsx", metadata={"source": "DATASUS"})
    to_geojson(df, "output.geojson", lat_col="LAT", lon_col="LON")
    ddl = to_sql(df, "table_name", dialect="duckdb")
"""

from pysus.api.export.csv_excel import to_csv, to_excel
from pysus.api.export.geojson import to_geojson
from pysus.api.export.sql import to_sql

__all__ = ["to_csv", "to_excel", "to_geojson", "to_sql"]
