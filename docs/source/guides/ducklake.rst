==========
DuckLake
==========

:class:`pysus.api.ducklake.client.DuckLake` is the S3-backed catalog —
the primary source. Parquet files live under hierarchical keys that
encode the file metadata::

   public/data/<origin>/<dataset>/<group>/<year>/<month>/<state>/<STEM>.parquet

Connecting
----------

Anonymous reads need no credentials:

.. code-block:: python

   import asyncio
   from pysus.api.ducklake.client import DuckLake

   async def main():
       async with DuckLake() as dl:
           ...

The catalog databases are downloaded to the PySUS cache on first use
(``<cachepath>/ducklake/*.duckdb``).

Listing datasets and files
--------------------------

.. code-block:: python

   datasets = await dl.datasets()           # 10 systems, lowercase names
   sinan = next(d for d in datasets if d.name == "sinan")
   files = await sinan.query(
       group="DENG",
       state=None,
       year=2024,
       month=None,
   )
   for f in files:
       print(f.path, f.size, f.rows, f.sha256)

Writing (maintenance)
---------------------

Uploading catalogs and files requires S3 credentials (``ACCESS_KEY`` /
``SECRET_KEY``); see :ref:`environment-variables`:

.. code-block:: python

   dl = DuckLake(update_on_close=True)
   await dl.login(access_key="<ak>", secret_key="<sk>")
   ...
   await dl.close(update_catalog=True)      # push catalog changes back to S3

Downloading
-----------

.. code-block:: python

   local = await f.download()               # streams the parquet from S3
   df = await local.load()

Old (pre-relayout) flat paths keep working: the bucket keeps alias
markers at former keys and downloads follow them transparently.
