============================
Data Quality & Profiling
============================

PySUS provides built-in tools for assessing data quality, profiling
datasets, and validating against expected schemas.

Column Statistics
-----------------

Get a per-column summary of types, nulls, uniques, and memory:

.. code-block:: python

   from pysus import column_stats

   stats = column_stats(df)
   print(stats[["column", "dtype", "null_pct", "unique_count", "memory_mb"]])

Returns a DataFrame with columns: ``column``, ``dtype``, ``null_count``,
``null_pct``, ``unique_count``, ``unique_pct``, ``memory_bytes``,
``memory_mb``, ``sample_value``. Sorted by memory usage (descending).

Missing Value Analysis
----------------------

.. code-block:: python

   from pysus import missing_values

   # Overall missing summary
   missing = missing_values(df)
   print(missing[missing["missing_pct"] > 0.1])  # columns with >10% missing

   # Group by state
   summary, by_state = missing_values(df, group_by="UF")

   # Filter to columns above a threshold
   high_missing = missing_values(df, threshold=0.3)  # only >30% missing

Quality Score
-------------

Compute an overall quality score (0-100) with breakdowns:

.. code-block:: python

   from pysus import quality_score

   score = quality_score(df)
   print(f"Overall: {score.overall:.0f}/100")
   print(f"  Completeness: {score.completeness:.0f}")
   print(f"  Validity:     {score.validity:.0f}")
   print(f"  Consistency:  {score.consistency:.0f}")

Scoring weights:

- **Completeness** (40%): average non-null percentage
- **Validity** (40%): values passing basic validation checks
- **Consistency** (20%): values matching expected patterns

Optionally pass a schema for stricter validation:

.. code-block:: python

   from pysus import quality_score, load_column_metadata

   columns = load_column_metadata("sinan", "Dengue")
   schema = {c["name"]: c for c in columns}
   score = quality_score(df, schema=schema)

Profile Reports
---------------

Generate comprehensive profiling reports in text, JSON, or HTML:

.. code-block:: python

   from pysus import profile_report

   # Text report to stdout
   report = profile_report(df, format="text")
   print(report)

   # Save as HTML
   profile_report(df, output="report.html", format="html")

   # Get as dict (for programmatic use)
   data = profile_report(df, format="json")

Schema Validation
-----------------

Validate data against expected rules:

.. code-block:: python

   from pysus import validate_data

   results = validate_data(df, dataset="SINAN")

   for r in results:
       status = "PASS" if r.passed else "FAIL"
       print(f"  [{status}] {r.column}: {r.rule} — {r.details}")

Built-in validation rules:

- **Age columns** (``IDADE``, ``NU_IDADE_N``): values 0-120
- **Date columns** (``DT_*``): ``YYYYMMDD`` format, reasonable range
- **Categorical columns** (``CS_*``): values in expected set

``validate_data`` returns a list of :class:`~pysus.api.quality.validation.ValidationResult`
objects, including both passing and failing rules.

Dataset Validation
------------------

Validate a dataset name against known PySUS datasets:

.. code-block:: python

   from pysus import validate_dataset, validate_origin

   canonical = validate_dataset("sinan")  # "SINAN"
   origin = validate_origin("ftp")        # "FTP"

Both raise :class:`~pysus.api.errors.ValidationError` with suggestions
for close matches.
