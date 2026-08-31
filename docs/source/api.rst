API Reference
=============

The ``pysus.api`` package provides a layered architecture for discovering,
downloading, and reading data from Brazilian public health databases
(DATASUS). It supports four remote data sources: FTP DataSUS, dados.gov.br,
OpenDataSUS (dadosabertos.saude.gov.br), and the DuckLake/S3 catalog mirror.

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
    ├── dadosgov/            # dados.gov.br API client
    ├── saude/               # OpenDataSUS catalog client
    ├── quality/             # Data quality and profiling
    ├── transform/           # Data transformation
    ├── export/              # Export to CSV/Excel/GeoJSON/SQL
    └── diff/                # DataFrame comparison

Quick Start
-----------

The simplest way to use PySUS is through an origin namespace, which makes the
data source explicit::

    import pysus

    df = pysus.ftp.sinan(disease="dengue", year=2023)
    df = pysus.saude.arboviroses(disease="dengue", year=2023)

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

Column Metadata
---------------

.. automodule:: pysus.api.columns
   :members:
   :undoc-members:

.. automodule:: pysus.api.mappings
   :members:
   :undoc-members:

Errors & Warnings
-----------------

.. automodule:: pysus.api.errors
   :members:
   :undoc-members:
   :show-inheritance:

Cache Management
----------------

.. automodule:: pysus.api.cache_utils
   :members:
   :undoc-members:

Input Validation
----------------

.. automodule:: pysus.api.validate
   :members:
   :undoc-members:

Data Quality
------------

.. automodule:: pysus.api.quality.statistics
   :members:
   :undoc-members:

.. automodule:: pysus.api.quality.missing
   :members:
   :undoc-members:

.. automodule:: pysus.api.quality.score
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: pysus.api.quality.validation
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: pysus.api.quality.profiling
   :members:
   :undoc-members:

Data Transformation
-------------------

.. automodule:: pysus.api.transform.aggregation
   :members:
   :undoc-members:

.. automodule:: pysus.api.transform.aliases
   :members:
   :undoc-members:

.. automodule:: pysus.api.transform.linking
   :members:
   :undoc-members:

.. automodule:: pysus.api.transform.masking
   :members:
   :undoc-members:

.. automodule:: pysus.api.transform.precision
   :members:
   :undoc-members:

.. automodule:: pysus.api.transform.streaming
   :members:
   :undoc-members:

.. automodule:: pysus.api.transform.units
   :members:
   :undoc-members:

Export
------

.. automodule:: pysus.api.export
   :members:
   :undoc-members:

.. automodule:: pysus.api.export.csv_excel
   :members:
   :undoc-members:

.. automodule:: pysus.api.export.geojson
   :members:
   :undoc-members:

.. automodule:: pysus.api.export.sql
   :members:
   :undoc-members:

Data Diff
---------

.. automodule:: pysus.api.diff.comparison
   :members:
   :undoc-members:
   :show-inheritance:

Metadata Models
---------------

.. automodule:: pysus.api.metadata.models
   :members:
   :undoc-members:

.. automodule:: pysus.api.metadata.report
   :members:
   :undoc-members:

OpenDataSUS Client
------------------

.. automodule:: pysus.api.saude.client
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: pysus.api.saude.schemas
   :members:
   :undoc-members:

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

Package Root
------------

.. automodule:: pysus
   :members:
   :undoc-members:
   :noindex:

DuckLake Internals
------------------

.. automodule:: pysus.api.ducklake.functional
   :members:
   :undoc-members:

.. automodule:: pysus.api.ducklake.catalog.adapters
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: pysus.api.ducklake.catalog.orm.default
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: pysus.api.ducklake.catalog.orm.dataset
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: pysus.api.ducklake.catalog.orm.columns
   :members:
   :undoc-members:
   :show-inheritance:

Legacy Data Readers
-------------------

.. automodule:: pysus.data.dbf_reader
   :members:
   :undoc-members:

CLI
---

.. automodule:: pysus.cli
   :members:
   :undoc-members:
