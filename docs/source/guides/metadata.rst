========
Metadata
========

Every remote entity in PySUS — clients, datasets, groups and files —
exposes a unified metadata surface via the ``.metadata`` property:

.. code-block:: python

   from pysus.api.saude import SaudeClient

   async with SaudeClient() as c:
       pkg = await c.fetch_dataset("arboviroses-dengue")
       # pkg.metadata is a MetadataBag

The MetadataBag
---------------

:class:`pysus.api.metadata.models.MetadataBag` organizes metadata into
eight typed facets:

- ``identity`` — name, slug, aliases, and ``cross_origin_id`` (the
  shared CKAN UUID that links the same dataset across the
  dados.gov.br and dadosabertos.saude.gov.br portals);
- ``description`` — title, long name, description, tags, themes;
- ``temporal`` — created/modified timestamps, periodicity, year/month;
- ``spatial`` — geographic scope, UF list, municipalities, state;
- ``provenance`` — origin, organization, author, contact, license;
- ``structure`` — columns, row count, file count, format;
- ``access`` — URL, format, size, auth requirement, policy;
- ``quality`` — content fingerprint, integrity, freshness.

Every bag serializes to JSON (``bag.to_dict()`` /
``MetadataBag.from_dict(...)``) so it can be persisted in the DuckLake
catalogs or snapshot files.

Extractors
----------

Each client ships its own extractors, one per entity type
(:class:`~pysus.api.metadata.extractors.MetadataExtractor`):

+-------------+------------------------------------------+
| Client      | Extractors                               |
+=============+==========================================+
| FTP         | ``FtpDatasetExtractor``,                 |
|             | ``FtpGroupExtractor``,                   |
|             | ``FtpFileExtractor``                     |
+-------------+------------------------------------------+
| DadosGov    | ``DadosGovDatasetExtractor``,            |
|             | ``DadosGovGroupExtractor``,              |
|             | ``DadosGovFileExtractor``                |
+-------------+------------------------------------------+
| DuckLake    | ``DuckLakeDatasetExtractor``,            |
|             | ``DuckLakeGroupExtractor``,              |
|             | ``DuckLakeFileExtractor``                |
+-------------+------------------------------------------+
| Saude       | ``SaudeDatasetExtractor``,               |
|             | ``SaudeGroupExtractor``,                 |
|             | ``SaudeFileExtractor``                   |
+-------------+------------------------------------------+

Concrete model classes declare their extractors via the
``extractor_types`` class attribute; the base classes only know that
*some* extractor exists:

.. code-block:: python

   file = await some_dataset.search(name="DENGBR25.csv.zip")[0]
   bag = file.metadata          # merged bag, cached
   print(bag.temporal.year)     # 2025 (from the filename formatter)
   print(bag.access.size_bytes)

Merging across origins
----------------------

:func:`~pysus.api.metadata.models.merge_bags` combines bags from
different origins with a documented per-facet precedence
(``roadmap_saude.md`` §1.7): Saude wins for descriptive fields,
DuckLake for structure and content fingerprints, and so on:

.. code-block:: python

   from pysus.api.metadata.models import merge_bags

   merged = merge_bags([ftp_file.metadata, saude_file.metadata])

   # Descriptive metadata comes from Saude, structure from DuckLake:
   print(merged.description.title)
   print(merged.structure.row_count)

Local files
-----------

Tabular local files (Parquet, CSV, DBF, ...) also expose ``.metadata``
computed from their content — columns, row count and size — without
any network access:

.. code-block:: python

   local = await file.download()
   print(local.metadata.structure.columns)

Curated column descriptions
---------------------------

The Saude catalog can apply curated column descriptions during a sync. The
SIM Declaração de Óbito resource is covered by
``pysus/api/saude/schemas/vigilanciameioambiente.yaml``. Its field names and
Portuguese descriptions are transcribed from the official SIM data dictionary
(updated July 2025); no records are bundled with PySUS.

The schema is keyed by the downloaded resource basename, so it can be applied
without relying on a particular data file or downloading a sample:

.. code-block:: python

   from pysus.api.saude.schemas import load_endpoint_columns

   columns = load_endpoint_columns(
       "vigilanciameioambiente", "DO24OPEN_csv"
   )
   assert any(column["name"] == "TIPOBITO" for column in columns)
