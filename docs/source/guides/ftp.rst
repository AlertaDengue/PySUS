====
FTP
====

:class:`pysus.api.ftp.client.FTP` browses the legacy DATASUS FTP server
(``ftp.datasus.gov.br``) — the complete historical archive: per-UF,
per-month ``.dbc`` files for every system.

Connecting
----------

Anonymous access, no credentials:

.. code-block:: python

   import asyncio
   from pysus.api.ftp.client import FTP

   async def main():
       ftp = FTP()
       await ftp.connect()
       try:
           ...
       finally:
           await ftp.close()

Listing datasets and files
--------------------------

.. code-block:: python

   datasets = await ftp.datasets()          # CIHA, CNES, IBGE, PNI, SIA, SIH, SIM, SINAN, SINASC
   sinan = next(d for d in datasets if d.name == "SINAN")
   content = await sinan.content            # groups (DENG, CHIK, ...) and directories

Each file carries parsed metadata (group, state, year, month) extracted
from the DATASUS filename conventions:

.. code-block:: python

   for item in content:
       for file in await item.files:
           print(file.name, file.size, file.modify, file.year, file.month, file.state)

Searching
---------

:class:`~pysus.api.models.BaseRemoteDataset.search` filters files by any
attribute:

.. code-block:: python

   files = await sinan.search(group="DENG", year=2024)

Downloading
-----------

.. code-block:: python

   local = await file.download()            # downloads to the PySUS cache
   df = await local.load()                  # parses the DBC into a DataFrame

``download()`` returns a wrapper (e.g. :class:`pysus.api.extensions.DBC`)
that decompresses and parses the legacy format on load.
