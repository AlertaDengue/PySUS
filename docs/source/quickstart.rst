==========
Quickstart
==========

Five minutes to your first DataFrame.

Install PySUS::

   pip install pysus

Then pick a client — the same file hierarchy
(:class:`~pysus.api.models.BaseRemoteDataset` →
:class:`~pysus.api.models.BaseRemoteGroup` →
:class:`~pysus.api.models.BaseRemoteFile`) is shared by all three
sources:

.. code-block:: python

   from pysus import sinan

   df = sinan(disease="deng", year=2024, as_dataframe=True)

S3 catalog (DuckLake) — the primary source
------------------------------------------

.. code-block:: python

   import asyncio
   from pysus.api.client import PySUS

   async def main():
       async with PySUS() as pysus:
           files = await pysus.query(
               dataset="sinan",
               group="DENG",
               year=2024,
           )
           for f in files:
               local = await pysus.download(f)
               print(local.path)

   asyncio.run(main())

FTP DATASUS
-----------

.. code-block:: python

   import asyncio
   from pysus.api.ftp.client import FTP

   async def main():
       ftp = FTP()
       await ftp.connect()
       try:
           for dataset in await ftp.datasets():
               if dataset.name != "SINAN":
                   continue
               for item in await dataset.content:
                   for file in await item.files:
                       if file.year == 2024 and "DENG" in file.name:
                           local = await file.download()
                           print(local.path)
       finally:
           await ftp.close()

   asyncio.run(main())

DadosGov API
------------

DadosGov requires an API token (see :ref:`environment-variables`):

.. code-block:: python

   import asyncio
   from pysus.api.dadosgov.client import DadosGov

   async def main():
       client = DadosGov()
       await client.connect(token="<your-token>")
       try:
           for dataset in await client.datasets():
               for group in await dataset.content:
                   for file in await group.files:
                       if file.year == 2024:
                           local = await file.download()
                           print(local.path)
       finally:
           await client.close()

   asyncio.run(main())

Next steps
----------

* :doc:`guides/pysus-orchestrator` — the PySUS class (query, download,
  read parquet)
* :doc:`guides/ftp` — FTP client details
* :doc:`guides/dadosgov` — DadosGov client details
* :doc:`guides/ducklake` — S3 catalog details
* :doc:`guides/files-and-formats` — DBF/DBC/CSV/ZIP/Parquet handling
* :doc:`guides/web-ui` — the Streamlit interface and CLI
