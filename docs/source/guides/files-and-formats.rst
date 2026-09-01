=================
Files & Formats
=================

PySUS handles the lifecycle of SUS data files: download, decompress,
convert, and read — transparently across five formats.

Supported Formats
-----------------

.. list-table::
   :header-rows: 1

   * - Format
     - Extension
     - Notes
   * - Parquet
     - ``.parquet``
     - Preferred. Columnar, compressed, fast. Default download target.
   * - DBC
     - ``.dbc``
     - DBF compressed with blast/SIGL. Auto-decompressed on read.
   * - DBF
     - ``.dbf``
     - dBASE III. Legacy DATASUS format. Read via ``DBFReader``.
   * - CSV
     - ``.csv``
     - Standard comma-separated. Used by OpenDataSUS/Saude.
   * - ZIP
     - ``.zip``
     - Archive container. Auto-extracted to find DBC/DBF/CSV inside.

Common Interface
----------------

All file handlers expose a common interface via
:class:`~pysus.api.extensions.ExtensionFactory`:

.. code-block:: python

   from pysus.api.extensions import ExtensionFactory

   # Auto-detect format from extension
   ext = await ExtensionFactory.instantiate(Path("DENGBR25.parquet"))

   # Full DataFrame
   df = await ext.load()

   # Chunked streaming
   async for chunk in ext.stream(chunk_size=50000):
       process(chunk)

   # Metadata without loading data
   print(ext.size, ext.rows, ext.columns)

DBC Decompression
-----------------

DBC files (``.dbc``) are blast-compressed dBASE files. PySUS handles
decompression automatically:

.. code-block:: python

   from pysus.api.extensions import ExtensionFactory

   ext = await ExtensionFactory.instantiate(Path("SINAN/DENGBR25.dbc"))
   df = await ext.load()  # decompresses, parses DBF, returns DataFrame

The decompression uses Python's ``blast`` module. For large files
(>100 MB), consider streaming:

.. code-block:: python

   async for chunk in ext.stream(chunk_size=100000):
       yield chunk

.. note::

   DBC files with NUL bytes or corrupted headers will raise
   ``ConversionError``. Re-downloading the file usually resolves this.

DBF Encoding
------------

DBF files use either ``latin-1`` or ``cp1252`` encoding. PySUS uses
``latin-1`` by default (since v2.6.3). If you encounter mojibake,
check the file's origin:

- **DATASUS FTP** → ``latin-1`` (correct default)
- **Legacy Windows exports** → ``cp1252`` (rare)

Parquet Conversion
------------------

Downloads are converted to Parquet by default. The conversion is handled
by :class:`~pysus.api.extensions.ExtensionFactory`:

.. code-block:: python

   from pysus import PySUS

   async with PySUS() as pysus:
       # download_to_parquet handles DBC→Parquet conversion
       parquet = await pysus.download_to_parquet(file)

       # Read the converted file
       df = pysus.read_parquet([parquet.path])

CSV Handling
------------

OpenDataSUS/Saude resources are distributed as CSV files. These are
read directly without conversion:

.. code-block:: python

   import pysus

   df = pysus.saude.arboviroses(disease="dengue", year=2024)

If you only want to list the CSV resources (without downloading), or work
with the downloaded files individually, use a ``FileBag``:

.. code-block:: python

   import pysus

   # Remote listing — nothing downloaded
   bag = pysus.saude.arboviroses(disease="dengue", download=False)
   print(bag)
   # Files[fa_casoshumanos_1994-2026.csv (remote), fa_epizpnh_1994-2026.csv (remote)]

   # Download and read as a DataFrame
   df = bag.download().to_dataframe()   # == bag.download().df

The ``FileBag`` API (``download``, ``download_one``, ``to_dataframe``,
``paths``, slice access) is shared across all origins and formats; see the
:ref:`FileBag Workflow <filebag-workflow>` tutorial for the full walkthrough.

ZIP Archives
------------

ZIP files are auto-extracted. The first DBC/DBF/CSV inside is used:

.. code-block:: python

   from pysus.api.extensions import ExtensionFactory

   ext = await ExtensionFactory.instantiate(Path("data.zip"))
   df = await ext.load()  # extracts and reads the first data file
