=================
Migrating to 2.x
=================

PySUS 2.x reworked the API around async clients and a unified file
hierarchy. This page summarizes what changed for 1.x users.

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

:class:`pysus.api.client.PySUS` manages all three sources (S3 catalog,
FTP, DadosGov), tracks downloads and converts to Parquet:

.. code-block:: python

   async with PySUS() as pysus:
       files = await pysus.query(dataset="sinan", year=2024)
       local = await pysus.download_to_parquet(files[0])

High-level convenience functions (``sinan(...)``, ``sim(...)``, …)
still exist in 2.x and return Parquet paths or DataFrames
(``as_dataframe=True``).

Cache and state
---------------

* Downloads live under ``PYSUS_CACHEPATH`` (default ``~/pysus``) —
  the ``PYSUS_CACHE_DIR`` name from 1.x is gone.
* The download history database is ``<cachepath>/config.db``
  (SQLite-backed in 1.x; now DuckDB-backed).

Downloads are converted to Parquet by default — the legacy
``.dbc``/``.dbf`` formats are parsed through
:mod:`pysus.api.extensions`.
