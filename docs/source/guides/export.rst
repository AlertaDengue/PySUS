======
Export
======

PySUS can export DataFrames to CSV, Excel, GeoJSON, SQL, and Parquet,
with automatic format detection from file extensions.

Auto-Detect Format
------------------

The ``export`` function picks the writer based on the file extension:

.. code-block:: python

   from pysus import export

   export(df, "output.csv")          # CSV + metadata sidecar
   export(df, "output.xlsx")         # Excel + metadata sheet
   export(df, "output.parquet")      # Parquet (native pandas)
   export(df, "output.json")         # JSON lines
   export(df, "output.sqlite", table_name="notifications")  # SQLite

CSV
---

.. code-block:: python

   from pysus import to_csv

   path = to_csv(df, "notifications.csv", encoding="utf-8")

Creates the CSV file plus a ``.metadata.json`` sidecar with row count,
column types, and any metadata dict passed to the ``metadata`` parameter:

.. code-block:: python

   to_csv(df, "data.csv", metadata={"source": "SINAN", "year": 2024})

Excel
-----

.. code-block:: python

   from pysus import to_excel

   path = to_excel(df, "report.xlsx", sheet_name="Notifications")

Metadata is included as a separate sheet. Requires ``openpyxl``
(``pip install pysus[web]`` or ``pip install openpyxl``).

GeoJSON
-------

Export with geographic coordinates for mapping:

.. code-block:: python

   from pysus import to_geojson

   path = to_geojson(
       df,
       "municipalities.geojson",
       lat_col="LATITUDE",
       lon_col="LONGITUDE",
       properties=["ID_AGRAVO", "DT_NOTIFIC", "UF"],
   )

Creates Point geometries from the lat/lon columns. Optionally include
a ``geocode_col`` for IBGE municipality codes as properties.

SQL DDL
-------

Generate a ``CREATE TABLE`` statement from the DataFrame schema:

.. code-block:: python

   from pysus import to_sql

   # Just the schema
   ddl = to_sql(df, "notifications", dialect="duckdb")
   print(ddl)

   # With INSERT statements
   ddl = to_sql(df, "notifications", dialect="duckdb", include_data=True)

Supported dialects: ``"duckdb"``, ``"mysql"``, ``"postgresql"``, ``"sqlite"``.

Compression
-----------

Pass compression to any writer via ``**kwargs``:

.. code-block:: python

   export(df, "data.csv.gz", compression="gzip")
   export(df, "data.parquet", compression="snappy")
