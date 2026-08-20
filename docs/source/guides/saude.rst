==============
OpenDataSUS
==============

:class:`pysus.api.saude.client.SaudeClient` talks to the portal of the
Brazilian Ministry of Health — `dadosabertos.saude.gov.br
<https://dadosabertos.saude.gov.br/>`_ — a Next.js frontend over a CKAN
backend. It exposes the catalog of 138 health datasets, their full CKAN
metadata, and the resource (file) downloads.

.. note::
   This is the *catalog* client (Stage 1 of ``roadmap_saude.md``). The
   structured DEMAS REST API (``apidadosabertos.saude.gov.br``) and the
   DuckLake sync integration ship in later stages.

No token required
-----------------

The portal is public — no authentication header is needed. Instantiate
the client and use it as an async context manager:

.. code-block:: python

   import asyncio
   from pysus.api.saude import SaudeClient

   async def main():
       async with SaudeClient() as client:
           ...

Listing datasets
----------------

.. code-block:: python

   # One page (20 entries) of the catalog
   page = await client.list_datasets(group="arboviroses")

   # All pages, lazily
   async for entry in client.iter_datasets(group="arboviroses"):
       print(entry.name, entry.title)

   # Filters: q (text), group, tag, fmt (resource format)
   page = await client.list_datasets(q="dengue", fmt="CSV")

Groups and tags
---------------

.. code-block:: python

   groups = await client.list_groups()   # 14 themes
   tags = await client.list_tags()

Fetching full metadata
----------------------

.. code-block:: python

   package = await client.fetch_dataset("arboviroses-dengue")
   print(package.title)              # "Sinan/Dengue"
   print(package.id)                 # CKAN UUID (shared with dados.gov.br)
   print(package.license_title)      # "Creative Commons Atribuição"
   print(package.periodicity)        # "Semanal"  (from extras[])
   print(package.contact)            # "arboviroses@saude.gov.br"

   resources = package.resources     # 19 fields each
   for res in resources[:5]:
       print(res.name, res.format, res.size)

Downloading resources
---------------------

.. code-block:: python

   # All CSV resources of the dengue dataset
   paths = await client.download_dataset(
       "arboviroses-dengue", fmt="CSV", dest_dir="./data"
   )

   # A single resource, by id or name
   path = await client.download_resource(
       "arboviroses-dengue", name="Dengue - 2024", dest_dir="./data"
   )

Resources with format ``API`` are documentation links and are skipped
automatically.

Caching
-------

The portal's Next.js ``buildId`` and every catalog response are cached
on disk (default TTL 24 h). Override with ``cache_dir`` and
``cache_ttl``:

.. code-block:: python

   from datetime import timedelta

   client = SaudeClient(
       cache_dir="./.cache/saude",
       cache_ttl=timedelta(hours=6),
   )
