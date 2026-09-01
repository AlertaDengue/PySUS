=================
Migrating to 2.x
=================

PySUS 2.x reworked the API around async clients, a unified file
hierarchy, and a growing ecosystem of metadata, export, and quality tools.
This page summarizes what changed for 1.x users.

Async-first clients
-------------------

Clients are now asynchronous and share one hierarchy
(:class:`~pysus.api.models.BaseRemoteDataset` →
:class:`~pysus.api.models.BaseRemoteGroup` →
:class:`~pysus.api.models.BaseRemoteFile`):

.. code-block:: python

   # 1.x
   from pysus.online_data import SINAN
   sinan = SINAN().load()

   # 2.x
   import asyncio
   from pysus.api.ftp.client import FTP

   async def main():
       ftp = FTP()
       await ftp.connect()
       try:
           for dataset in await ftp.datasets():
               ...
       finally:
           await ftp.close()

   asyncio.run(main())

Central orchestrator
--------------------

:class:`pysus.api.client.PySUS` manages all four sources (S3 catalog,
FTP, DadosGov, OpenDataSUS), tracks downloads and converts to Parquet:

.. code-block:: python

   async with PySUS() as pysus:
       files = await pysus.query(dataset="sinan", year=2024)
       local = await pysus.download_to_parquet(files[0])

Origin-namespaced fetchers (``pysus.ftp.sinan``, ...) exist in addition to
the legacy high-level convenience functions (``sinan(...)``, ``sim(...)``,
…). The namespaced form makes the data source explicit and is the
recommended way to fetch data. See below for the flat → namespaced migration.

OpenDataSUS (Saude) client
--------------------------

PySUS 2.9+ added a public client for the Ministry of Health's open-data
portal (``dadosabertos.saude.gov.br``) — no token required:

.. code-block:: python

   import pysus

   df = pysus.saude.arboviroses(disease="dengue", year=2024)
   df = pysus.saude.vacinacao(state="SP", year=2024)

Or use the low-level client:

.. code-block:: python

   from pysus.api.saude import SaudeClient

   async with SaudeClient() as client:
       page = await client.list_datasets(group="arboviroses")
       for entry in page:
           print(entry.name, entry.title)

Migrating from the flat fetchers to the origin namespaces
---------------------------------------------------------

The legacy flat fetchers (``pysus.sinan``, ``pysus.arboviroses``, …) still
work unchanged, but each call now emits a ``PySUSWarning`` pointing you to
the origin-namespaced equivalent. Migrate by prefixing the fetcher with its
origin namespace:

.. code-block:: python

   # Deprecated (flat)                                  # Recommended (namespaced)
   from pysus import sinan                              pysus.ftp.sinan(disease="deng", year=2024)
   df = sinan(disease="deng", year=2024)                pysus.ftp.sim(state="SP", year=2024)
   df = sim(state="SP", year=2024)                      pysus.dadosgov.sinasc(state="SP", year=2024)
   df = sinasc(state="SP", year=2024)                   from pysus.ftp import sinan   # also works
   df = arboviroses(disease="dengue", year=2024)        pysus.saude.arboviroses(disease="dengue", year=2024)

The ``source`` parameter controls where data is read: ``source="catalog"``
(default) serves the S3/Parquet mirror (same results as today's default);
``source="origin"`` queries the origin server directly. The Saude portal has
no catalog mirror, so ``pysus.saude.*`` always queries the CKAN portal.

Unified metadata layer
----------------------

Every remote entity exposes a ``.metadata`` property returning a
:class:`~pysus.api.metadata.models.MetadataBag` with eight typed facets
(identity, description, temporal, spatial, provenance, structure,
access, quality):

.. code-block:: python

   bag = file.metadata
   print(bag.description.title)
   print(bag.structure.row_count)

Bags from different origins can be merged:

.. code-block:: python

   from pysus.api.metadata.models import merge_bags
   merged = merge_bags([ftp_file.metadata, saude_file.metadata])

First-run experience
--------------------

On first import, PySUS prints a welcome message with the cache path
and a pointer to ``pysus.info()``:

.. code-block:: python

   import pysus
   pysus.info()  # prints a table of available datasets

Cache and state
---------------

* Downloads live under ``PYSUS_CACHEPATH`` (default ``~/pysus``) —
  the ``PYSUS_CACHE_DIR`` name from 1.x is gone.
* The download history database is ``<cachepath>/config.db``
  (SQLite-backed in 1.x; now DuckDB-backed).

Downloads are converted to Parquet by default — the legacy
``.dbc``/``.dbf`` formats are parsed through
:mod:`pysus.api.extensions`.

TOML configuration
------------------

PySUS reads ``~/.config/pysus/config.toml`` for persistent settings:

.. code-block:: bash

   pysus configure  # interactive wizard

Or set values directly:

.. code-block:: toml

   [download]
   timeout = 300
   max_concurrent = 5

   [cache]
   path = "/data/pysus"

CLI commands
------------

PySUS ships a CLI built with Typer:

.. code-block:: bash

   pysus version              # installed version
   pysus web                  # Streamlit web interface
   pysus web -p 8080          # custom port
   pysus cache status         # cache statistics
   pysus cache clear          # clear all cached files
   pysus configure            # interactive configuration

Friendly errors
---------------

PySUS raises typed exceptions for common failure modes:

.. code-block:: python

   from pysus import (
       PySUSError,
       ConnectionError,
       DownloadError,
       ValidationError,
       FormatError,
   )

   try:
       df = sinan(disease="invalid", year=2024)
   except ValidationError as e:
       print(e)  # includes hint about valid choices

Progress bars
-------------

Progress bars are enabled by default. Control them with:

.. code-block:: python

   from pysus import disable_progress_bars, enable_progress_bars

   disable_progress_bars()
   # ... do work ...
   enable_progress_bars()

Export
------

DataFrames can be exported to multiple formats:

.. code-block:: python

   from pysus import to_csv, to_excel, to_geojson, to_sql, export

   to_csv(df, "output.csv")
   to_excel(df, "output.xlsx")
   to_geojson(df, "output.geojson", lat_col="LATITUDE", lon_col="LONGITUDE")
   export(df, "output.csv", compression="gzip")

Data quality
------------

.. code-block:: python

   from pysus import column_stats, missing_values, quality_score, validate_data

   stats = column_stats(df)
   missing = missing_values(df)
   score = quality_score(df)  # 0-100
   issues = validate_data(df, dataset="SINAN")

Column metadata
---------------

.. code-block:: python

   from pysus import search_columns, load_column_metadata

   # Search for a column across all datasets
   results = search_columns("CON_CLASSI")
   for r in results:
       print(r.dataset, r.name, r.categories)

   # Load curated columns for a specific dataset
   columns = load_column_metadata("sinan", "Dengue")

Parallel downloads
------------------

.. code-block:: python

   async with PySUS() as pysus:
       files = await pysus.query(dataset="sinan", group="DENG", year=2024)
       downloaded = await pysus.download_many(files, max_concurrent=5)
