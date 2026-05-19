API Reference
=============

The ``pysus.api`` package provides a layered architecture for discovering,
downloading, and reading data from Brazilian public health databases
(DATASUS). It supports three remote data sources.

Architecture Overview
---------------------

The package is organized into a hierarchy of abstract base classes and
concrete implementations::

    pysus/api/
    ├── __init__.py          # Package entry (re-exports PySUS)
    ├── client.py            # Main PySUS orchestrator
    ├── extensions.py        # File format handlers
    ├── models.py            # Abstract base classes
    ├── types.py             # Type aliases
    ├── _impl/
    │   └── databases.py     # High-level convenience functions
    ├── ducklake/            # S3 DuckLake catalog client
    ├── ftp/                 # FTP client
    └── dadosgov/            # dados.gov.br API client

Quick Start
-----------

The simplest way to use PySUS is via the high-level convenience
functions::

    from pysus import sinan

    df = sinan(disease="dengue", year=2023)

Or with the async API::

    from pysus.api.client import PySUS

    async with PySUS() as pysus:
        files = await pysus.query(dataset="sinan", group="DENG", year=2023)
        for f in files:
            await pysus.download(f)


Main Client
-----------

.. automodule:: pysus.api.client
   :members:
   :undoc-members:
   :show-inheritance:

Types
-----

.. automodule:: pysus.api.types
   :members:
   :undoc-members:

Utilities
---------

.. automodule:: pysus.api.utils
   :members:
   :undoc-members:

File Format Handlers
--------------------

.. automodule:: pysus.api.extensions
   :members:
   :undoc-members:
   :show-inheritance:

Abstract Base Models
--------------------

.. automodule:: pysus.api.models
   :members:
   :undoc-members:
   :show-inheritance:

High-Level Data Functions
-------------------------

.. automodule:: pysus.api._impl.databases
   :members:
   :undoc-members:
   :show-inheritance:

DuckLake Client
---------------

.. automodule:: pysus.api.ducklake.client
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: pysus.api.ducklake.catalog
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: pysus.api.ducklake.models
   :members:
   :undoc-members:
   :show-inheritance:

FTP Client
----------

.. automodule:: pysus.api.ftp.client
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: pysus.api.ftp.databases
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: pysus.api.ftp.models
   :members:
   :undoc-members:
   :show-inheritance:

DadosGov Client
---------------

.. automodule:: pysus.api.dadosgov.client
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: pysus.api.dadosgov.databases
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: pysus.api.dadosgov.models
   :members:
   :undoc-members:
   :show-inheritance:
