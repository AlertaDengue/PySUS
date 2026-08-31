========================
The PySUS Orchestrator
========================

:class:`pysus.api.client.PySUS` is the central entry point: it manages
the four clients (S3 catalog, FTP, DadosGov, OpenDataSUS), tracks local
downloads, converts files to Parquet, and reads them back.

Connecting
----------

.. code-block:: python

   import asyncio
   from pysus.api.client import PySUS

   async def main():
       async with PySUS() as pysus:
           ...

The context manager connects the DuckLake catalog and cleans up every
client on exit.

Querying the catalog
--------------------

.. code-block:: python

   files = await pysus.query(
       dataset="sinan",      # SINAN, SIM, SINASC, SIA, SIH, CIHA, CNES, PNI, IBGE
       group="DENG",         # disease/system group code
       state="SP",           # optional UF filter
       year=2024,            # optional
       month=1,              # optional
       client="ftp",         # optional: ftp | dadosgov | ducklake
   )

Downloading
-----------

Files are cached locally (see :class:`pysus.api.client.LocalFileState`);
re-downloads are avoided when the cached file matches:

.. code-block:: python

   local = await pysus.download(f)                 # raw file wrapper
   parquet = await pysus.download_to_parquet(f)    # converts to parquet

Both return wrappers from :mod:`pysus.api.extensions` that expose
``load()`` (full DataFrame), ``stream()`` (chunked) and metadata
(``size``, ``rows``, ``columns``).

Parallel downloads
------------------

For batch downloads, use ``download_many`` to fetch files concurrently:

.. code-block:: python

   downloaded = await pysus.download_many(files, max_concurrent=5)

Reading parquet files
---------------------

.. code-block:: python

   df = pysus.read_parquet(paths)                              # union (default)
   df = pysus.read_parquet(paths, mode="intersection")         # common columns
   df = pysus.read_parquet(paths, mode="strict")               # schemas must match
   df = pysus.read_parquet(paths, sql="SELECT * WHERE idade > 60")

``read_parquet`` returns a DataFrame when geocode columns are found
(the IBGE verification digit is applied automatically); otherwise a
DuckDB connection is returned (use ``.df()`` on it).

Searching for datasets
----------------------

Use ``search`` for keyword-based discovery across all origins:

.. code-block:: python

   results = await pysus.search("dengue")

Or use the standalone ``list_files`` function (scoped per origin):

.. code-block:: python

   import pysus
   df = pysus.ftp.list_files("SINAN", group="DENG", year=2024)

Local download history
----------------------

.. code-block:: python

   hierarchy = pysus.get_local_hierarchy()      # {client: {dataset: {group: [...]}}}
   completed = pysus.get_completed_remote_paths()

The cache location is controlled by the ``PYSUS_CACHEPATH``
environment variable; the state database lives at
``<cachepath>/config.db``.
