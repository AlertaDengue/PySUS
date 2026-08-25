=========
Tutorials
=========

Step-by-step usage examples.

Simplified Database Functions
-----------------------------

.. code-block:: python

   from pysus import sinan, sinasc, sim, sih, sia, pni, ibge, cnes, ciha

   # Download SINAN Dengue data
   df = sinan(disease="deng", year=2000)

   # Multiple years
   df = sinan(disease="deng", year=[2023, 2024])

   # SINASC births for São Paulo
   df = sinasc(state="SP", year=[2020, 2021, 2022, 2023])

   # SIM mortality data
   df = sim(state="SP", year=2024)

   # SIH hospitalizations with month filter
   df = sih(state="SP", year=2024, month=[1, 2, 3])

   # CNES health facilities
   df = cnes(state="SP", year=2024, month=1)

OpenDataSUS (Saude) Functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from pysus import arboviroses, vacinacao, assistencia_saude

   # Dengue/Chik/Zika notifications from OpenDataSUS
   df = arboviroses(disease="dengue", year=2024)

   # Vaccination coverage
   df = vacinacao(state="SP", year=2024)

   # Hospital and health facility data
   df = assistencia_saude(state="SP", year=2024)

   # Primary care (Previne Brasil)
   df = atencao_primaria(state="SP", year=2024)

   # Nutrition surveillance
   df = sisvan(state="SP", year=2024)

Discovery
---------

.. code-block:: python

   from pysus import info_table, search, list_files

   # Browse available datasets
   info_table()

   # Search for datasets by keyword
   results = search("dengue")

   # List files in a dataset
   df = list_files("SINAN", group="DENG", year=2024)

Using the PySUS Client
----------------------

.. code-block:: python

   from pysus import PySUS

   async def main():
       async with PySUS() as pysus:
           # Query DuckLake catalog
           files = await pysus.query(
               dataset="sinan",
               group="DENG",
               state="SP",
               year=2024,
           )

           # Download files
           for f in files:
               local = await pysus.download(f)
               print(local.path)

           # Read multiple parquet files
           import glob
           paths = glob.glob("/cache/sinan/**/*.parquet")
           df = pysus.read_parquet(paths, mode="union")

Parallel Downloads
^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Download many files in parallel
   downloaded = await pysus.download_many(files, max_concurrent=5)

read_parquet Modes
^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Union (default) - all columns from any file
   df = pysus.read_parquet(paths, mode="union")

   # Intersection - only common columns across all files
   df = pysus.read_parquet(paths, mode="intersection")

   # Strict - raises error if schemas don't match
   df = pysus.read_parquet(paths, mode="strict")

   # With custom SQL filter
   df = pysus.read_parquet(paths, sql="SELECT * WHERE column > 100")

Data Quality
------------

.. code-block:: python

   from pysus import column_stats, missing_values, quality_score, validate_data

   # Per-column statistics (types, nulls, uniques, sample values)
   stats = column_stats(df)

   # Missing value summary
   missing = missing_values(df)

   # Overall quality score (0-100)
   score = quality_score(df)

   # Validate data against expected schema
   issues = validate_data(df, dataset="SINAN")

Data Transformation
-------------------

.. code-block:: python

   from pysus import (
       aggregate_by_age_group,
       aggregate_by_period,
       aggregate_by_state,
       detect_units,
       optimize_memory,
       rename_columns,
       to_english,
   )

   # Aggregate cases by age group
   grouped = aggregate_by_age_group(df, age_col="IDADE", period_col="DT_NOTIFIC")

   # Detect measurement units in columns
   units = detect_units(df)

   # Rename columns with a mapping
   df = rename_columns(df, mapping={"DT_NOTIFIC": "notification_date"})

   # Convert Portuguese column/value names to English
   df_en = to_english(df)

   # Optimize memory usage
   df = optimize_memory(df)

Column Metadata
---------------

.. code-block:: python

   from pysus import search_columns, load_column_metadata, available_databases

   # List curated schema databases
   print(available_databases())  # ["sim", "sinan"]

   # Load columns for a specific SINAN disease
   columns = load_column_metadata("sinan", "Dengue")

   # Search for a column across all datasets
   results = search_columns("CON_CLASSI")
   for r in results:
       print(r.dataset, r.name, r.categories)

Export
------

.. code-block:: python

   from pysus import export, to_csv, to_excel, to_geojson, to_sql

   # Export to various formats
   to_csv(df, "output.csv")
   to_excel(df, "output.xlsx")
   to_geojson(df, "output.geojson", lat_col="LATITUDE", lon_col="LONGITUDE")

   # Full export with options
   export(df, "output.csv", compression="gzip")

Data Diff
---------

.. code-block:: python

   from pysus import diff_dfs, diff_summary, diff_rows

   # Compare two DataFrames
   diff = diff_dfs(df_old, df_new)

   # Summary of differences
   summary = diff_summary(df_old, df_new)

   # Row-level differences
   rows = diff_rows(df_old, df_new)

Cache Management
----------------

.. code-block:: python

   from pysus import cache_status, clear_cache

   # Show cache statistics
   cache_status()

   # Clear all cached files
   clear_cache()
