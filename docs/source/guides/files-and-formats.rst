==================
Files and Formats
==================

Every download (any client) returns a local file wrapper from
:mod:`pysus.api.extensions`, chosen by extension via
:class:`pysus.api.extensions.ExtensionFactory`.

Supported formats
-----------------

* ``.dbc`` / ``.dbf`` — legacy DATASUS formats (:class:`DBC`, :class:`DBF`)
* ``.csv`` — plain text tabular (:class:`CSV`)
* ``.zip`` — compressed archives (:class:`BaseCompressedFile`)
* ``.parquet`` — the canonical format (:class:`Parquet`)

Common interface
----------------

.. code-block:: python

   local = await remote_file.download()

   df = await local.load()              # full DataFrame
   async for chunk in local.stream():   # chunked iteration
       process(chunk)

   print(local.name, local.extension, local.size, local.modify)

Tabular files also expose:

.. code-block:: python

   print(local.columns)                 # list of Column(name, description, dtype)
   print(local.rows)                    # row count

Converting to Parquet
---------------------

Any tabular or compressed file converts with one call:

.. code-block:: python

   parquet = await local.to_parquet()                       # next to the source
   parquet = await local.to_parquet(output_path="out.parquet")

The conversion streams in chunks (default 10 000 rows), so large DBC
files never need to fit in memory at once.
