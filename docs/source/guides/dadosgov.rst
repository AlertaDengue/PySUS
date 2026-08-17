=========
DadosGov
=========

:class:`pysus.api.dadosgov.client.DadosGov` talks to the dados.gov.br
open-data API. It publishes national, recent CSV/JSON/XML extracts
(mainly SINAN arboviroses, SIM, SINASC, PNI, CNES).

Token
-----

The API requires a token (see :ref:`environment-variables`). Pass it to
``connect``:

.. code-block:: python

   import asyncio
   from pysus.api.dadosgov.client import DadosGov

   async def main():
       client = DadosGov()
       await client.connect(token="<your-token>")
       try:
           ...
       finally:
           await client.close()

Listing datasets and files
--------------------------

.. code-block:: python

   datasets = await client.datasets()       # CNES, PNI, SIA, SIM, SINAN, SINASC, COVID19
   sinan = next(d for d in datasets if d.name == "SINAN")
   for group in await sinan.content:
       print(group.name, group.long_name)
       for file in await group.files:
           print(file.name, file.size, file.year, file.month, file.state)

Format triplets
---------------

The portal serves the same data as ``csv.zip``, ``json.zip`` and
``xml.zip``; PySUS keeps the CSV and hides the others, so you see each
logical file once.

Searching the raw API
---------------------

Lower-level helpers expose the raw portal:

.. code-block:: python

   conjuntos = await client.list_datasets(nome_conjunto="sinan")
   conjunto = await client.get_dataset("<dataset-id>")

Downloading
-----------

.. code-block:: python

   local = await file.download()
   df = await local.load()

Downloads go through the authenticated session (the token header is
attached automatically).
