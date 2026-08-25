====================
Data Transformation
====================

PySUS includes tools for aggregating, renaming, linking, masking, and
optimizing SUS datasets without leaving Python.

Aggregation
-----------

Aggregate SUS data by state, age group, or time period:

.. code-block:: python

   from pysus import (
       aggregate_by_state,
       aggregate_by_age_group,
       aggregate_by_period,
   )

   # Count cases by state
   by_state = aggregate_by_state(df, value_col="ID_AGRAVO", agg_func="count")

   # Sum hospitalizations by age group
   by_age = aggregate_by_age_group(
       df,
       age_col="IDADE",
       value_col="DIAG_PRINC",
       bins=[0, 5, 15, 30, 50, 70, 120],
   )

   # Monthly notifications
   by_month = aggregate_by_period(
       df,
       date_col="DT_NOTIFIC",
       value_col="ID_AGRAVO",
       freq="M",       # "M"onthly, "Q"uarterly, "Y"early
   )

``aggregate_by_period`` expects date columns in ``YYYYMMDD`` format
(the standard across SINAN, SIM, SINASC, SIH, and SIA).

Column Aliasing and Renaming
----------------------------

SUS datasets use Portuguese column names that change across form
editions. ``rename_columns`` and ``get_aliases`` handle this:

.. code-block:: python

   from pysus import rename_columns, get_aliases

   # See what a column was called in previous editions
   aliases = get_aliases("sinan", "DT_NOTIFIC")
   # ["DT_NOTIFIC", "DT_NOT_N"]

   # Rename with historical aliases (all old names → canonical)
   df = rename_columns(df, database="sinan")

   # Or use a custom mapping
   df = rename_columns(df, mapping={
       "DT_NOTIFIC": "notification_date",
       "DT_SIN_PRI": "symptom_onset_date",
   })

Cross-Dataset Linking
---------------------

Link two SUS datasets on shared columns (municipality code, CPF, etc.):

.. code-block:: python

   from pysus import get_linking_keys, link_datasets

   # Discover what columns two datasets share
   keys = get_linking_keys("sinan", "sim")
   # ["CID_MUNIC", "DT_NOTIFIC", ...]

   # Merge on those keys
   merged = link_datasets(notifications, deaths, on=keys, how="left")

``link_datasets`` handles column name conflicts by adding ``_left``
and ``_right`` suffixes.

Sensitive Data Masking
----------------------

Protect sensitive columns (CPF, names, addresses) before sharing:

.. code-block:: python

   from pysus import mask_data, unmask_data

   # Encrypt sensitive columns (auto-detects CPF/NOME patterns)
   masked_df, key = mask_data(df, method="encrypt")

   # Or target specific columns
   masked_df, key = mask_data(
       df,
       columns=["CPF", "NOME", "ENDereco"],
       method="hash",     # "encrypt", "hash", or "redact"
   )

   # Reverse later with the key
   original_df = unmask_data(masked_df, columns=["CPF", "NOME"], key=key)

Three masking methods:

- ``"encrypt"`` — reversible AES encryption (default)
- ``"hash"`` — one-way SHA-256 (irreversible)
- ``"redact"`` — replace with ``***`` (irreversible, simplest)

Memory Optimization
-------------------

SUS datasets are large. Optimize memory before analysis:

.. code-block:: python

   from pysus import optimize_memory, set_precision

   # Auto-downcast int/float to smallest sufficient type
   df = optimize_memory(df)

   # Set specific numeric precision
   df = set_precision(df, precision="float32")

Typical memory reduction: 30-60% for datasets with mixed int/float columns.

Unit Detection
--------------

Detect units of measurement in numeric columns:

.. code-block:: python

   from pysus import detect_units

   units = detect_units(df)
   for u in units:
       print(f"{u.column}: {u.unit} (confidence: {u.confidence:.0%})")

``detect_units`` uses column names, value ranges, and any available
metadata to infer units (e.g., ``"kg"``, ``"mg/dL"``, ``"years"``).

Streaming Large Files
---------------------

For files too large to fit in memory, process them in chunks:

.. code-block:: python

   from pysus import stream_parquet

   for chunk in stream_parquet("large_file.parquet", chunk_size=50000):
       # Process each chunk
       results = chunk.groupby("UF").size()

``stream_parquet`` yields DataFrames of ``chunk_size`` rows, reading
only the specified columns if given:

.. code-block:: python

   for chunk in stream_parquet(
       "SIHSR2401.parquet",
       chunk_size=10000,
       columns=["UF", "IDADE", "DIAG_PRINC"],
   ):
       process(chunk)
