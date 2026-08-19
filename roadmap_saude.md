# Roadmap: Integrating `dadosabertos.saude.gov.br` (Portal de Dados Abertos do SUS) into PySUS

> **Note on metadata.** The most important piece of this integration is
> the unified metadata abstraction. Every client (FTP, DadosGov,
> DuckLake, Saude) extracts metadata from very different sources, but the
> shape that flows through PySUS must be one. **§1 Metadata architecture**
> is the new foundation; the rest of the roadmap assumes it exists.

This roadmap defines how to add the Ministry of Health open-data portal
(`dadosabertos.saude.gov.br`) as a new first-class client alongside the
existing `FTP` (DATASUS) and `DadosGov` (federal portal) clients, and how
to propagate it through every layer of PySUS — `pysus/api`,
`pysus/management`, DuckLake catalogs and CLI.

The existing `roadmap.md` covers the broader cross-client sync work and
should be read alongside this one; this file adds a **new origin**
(`saude`) on top of the stage infrastructure already in place.

---

## 0. The portal has two surfaces — treat them as one

`dadosabertos.saude.gov.br` is a CKAN catalog (HTML-only — there is **no
CKAN Action API** at `/api/3/action/...`, all 138 datasets are reachable
only through HTML scraping or hard-coded IDs). Its sibling service
`apidadosabertos.saude.gov.br` (DEMAS API, Flask/Connexion v5.32.12) exposes
**87 paginated JSON endpoints** under `/static/swagger.json`.

| Concern | CKAN catalog (`dadosabertos.saude.gov.br`) | DEMAS REST (`apidadosabertos.saude.gov.br`) |
|---|---|---|
| Discover datasets | ✅ `https://dadosabertos.saude.gov.br/dataset?rows=...` (HTML) | ⚠️ only through the embedded Swagger |
| Metadata per dataset | ✅ title, organização, group, license, formato, periodicidade, tags, resources | ⚠️ only `summary` + `tags` per endpoint; **no response schema** |
| Metadata per file/resource | ✅ `resources[]` with `url`, `name`, `format`, `created`, `last_modified`, `size` | n/a — endpoints are paginated JSON, not file resources |
| Column-level metadata | ❌ no CKAN datastore | ❌ Swagger `responses: type=None ref=None` for every endpoint |
| Auth | none | none (no token required) |
| Pagination | n/a (HTML scrape only) | `limit ≤ 1000`, `offset` |

**Decision:** implement a single client `DadosAbertosSaude`
(`pysus/api/saude/`) with two responsibilities:

1. **Catalog discovery** — uses the portal's **Next.js data layer**
   (`/_next/data/<buildId>/dataset.json`) instead of HTML scraping
   (see §0.5 below) and parses the DEMAS Swagger spec to build the
   in-memory `Dataset`/`Group`/`File` tree. The catalog is the
   **authoritative list** of datasets and groups.
2. **Row access** — calls the DEMAS JSON endpoints directly, with no
   download to disk. Files are paginated JSON pages, **not** downloadable
   artifacts in the same sense as FTP/DadosGov files.

Both surfaces are read-only and tokenless; one `connect()` suffices.

### 0.5 Lessons from the `epidatasets` reference implementation

The companion project `fccoelho/epidemiological-datasets` already ships
an `OpenDataSUSAccessor` (in
`epidatasets/sources/opendatasus.py`) that talks to this same portal.
It pre-validates a number of decisions and surfaces concrete
implementation details that should be reused rather than reinvented.
Most importantly, it confirms three things:

#### 0.5.1 The portal is Next.js, not vanilla CKAN

The CKAN Action API at `/api/3/action/*` returns 404. The portal is a
**Next.js frontend over a CKAN backend**, served via the Next.js
data layer that Next.js pages normally hit for client-side hydration.
The accessor in `epidatasets` uses three endpoints:

| Endpoint | Purpose |
|---|---|
| `GET https://dadosabertos.saude.gov.br/` | homepage — extract `<buildId>` from `<script id="__NEXT_DATA__" type="application/json">` |
| `GET /_next/data/<buildId>/dataset.json?q=…&groups=…&tags=…&res_format=…&page=N` | paginated catalog listing (20 per page) |
| `GET /_next/data/<buildId>/dataset/<slug>.json?slug=<slug>` | full CKAN package (metadata + resources) |

`<buildId>` rotates on every frontend deploy, so it must be scraped
from the homepage and cached. PySUS reuses the exact same regex
extractor (`_NEXT_DATA_RE` in the reference), wrapped in async httpx.

**This replaces the "HTML scraper" outlined in the original §0 — much
more robust.**

#### 0.5.2 The CKAN package schema is rich

A `/dataset/<slug>.json` response exposes **30 keys**:

```
author              author_email          creator_user_id       id
isopen              license_id            license_title         license_url
maintainer          maintainer_email      metadata_created      metadata_modified
name                notes                 num_resources         num_tags
organization        owner_org             private               state
title               type                  url                   version
extras[]            groups[]              tags[]                resources[]
relationships_as_object[]                  relationships_as_subject[]
```

Plus the same packages surface in `/dataset.json` paginated output
(projected to `name`, `title`, `notes`, `formats[]`, `groups[]`,
`tags[]`). The `extras[]` field is the source of:

- `Contato` (e.g. `arboviroses@saude.gov.br`) — for `provenance.contact`
- `Frequência de atualização` (e.g. `Semanal`) — for
  `temporal.periodicity` in the `MetadataBag`

The full mapping into the `MetadataBag` from §1.3:

| CKAN field | `MetadataBag` facet |
|---|---|
| `id` (UUID) | `identity.cross_origin_id` (see §0.5.4) |
| `name` | `identity.slug` |
| `title` | `description.title` |
| `notes` | `description.description` |
| `groups[].display_name` | `description.theme` |
| `tags[].display_name` | `description.tags` |
| `author` | `provenance.author` |
| `organization.display_name` | `provenance.organization` |
| `maintainer`, `maintainer_email` | `provenance.maintainer` |
| `license_id`, `license_title`, `license_url` | `provenance.license` |
| `metadata_created` | `temporal.created` |
| `metadata_modified` | `temporal.modified` |
| `extras[Frequência de atualização]` | `temporal.periodicity` |
| `extras[Contato]` | `provenance.contact` |
| `num_resources` | `structure.file_count` |
| `private`, `state`, `isopen` | `access.policy` |
| `metadata_modified` for `resources[]` | `temporal.modified` per resource |

A resource (`resources[]` element) carries 19 keys:

```
cache_last_updated   cache_url             created
datastore_active     description           format
hash                 id                    last_modified
metadata_modified    mimetype              mimetype_inner
name                 package_id            position
resource_type        size                  state
url                  url_type
```

These populate the file-level `MetadataBag`:

| Resource field | `MetadataBag` facet |
|---|---|
| `id` (UUID) | `identity.cross_origin_id` |
| `name` | `identity.name` |
| `description` | `description.description` |
| `format` | `access.format` |
| `url` | `access.url` |
| `size` | `access.size_bytes` |
| `created`, `last_modified`, `metadata_modified` | `temporal.*` |
| `mimetype` | `access.mime_type` |
| `hash` | `quality.integrity_hint` |
| `position` | `structure.position` (display order) |

#### 0.5.3 Reusable pieces from the reference

Every helper that does not depend on `pandas.DataFrame` as the
return type should be ported (with the obvious async + typing
changes). Concrete plan:

| Reference (`epidatasets`) | Target (`pysus/api/saude/`) |
|---|---|
| `_NEXT_DATA_RE` | `next_data.py::extract_build_id` |
| `_get_build_id()` (sync, with cache) | `catalog.py::get_build_id()` (async, with cache) |
| `_get_json()` (cache + retries) | `catalog.py::fetch_json()` — base for all catalog calls |
| `_is_cache_valid()` / `_read_cache()` / `_write_cache()` | `catalog.py::cache_*` |
| `_search_params()` | `catalog.py::CatalogQuery` (pydantic) |
| `_fetch_catalog_page()` | `catalog.py::fetch_catalog_page()` |
| `_packages_to_frame()` | replaced — return `list[ConjuntoDados]` instead of a DataFrame |
| `list_datasets()` / `list_datasets_all()` | `catalog.py::list_datasets()` / `list_all_datasets()` |
| `list_groups()` / `list_tags()` | `catalog.py::list_groups()` / `list_tags()` |
| `get_dataset()` (full package) | `catalog.py::fetch_dataset(slug)` — returns the raw dict, then handed to `SaudeDatasetExtractor` |
| `get_dataset_metadata()` (field/value) | replaced by `SaudeDatasetExtractor.extract()` → `MetadataBag` |
| `get_resources()` | `catalog.py::fetch_resources(slug)` |
| `_filename_for()` | reused verbatim in `saude/download.py` |
| `download_resource()` / `download_dataset()` | `saude/download.py::download_resource()` / `download_dataset()` (sync wrapper kept for parity) |

The DataFrame-shaped convenience helpers (`_packages_to_frame`,
`get_dataset_metadata` as a DataFrame) are **not** ported — PySUS
already has `read_parquet()` for tabular views; the Saude client
should return rich `MetadataBag` objects instead of DataFrames.

The reference uses `requests`; PySUS uses `httpx` everywhere else, so
the helper ports 1-for-1 except for the transport.

#### 0.5.4 The two portals share the same CKAN package UUIDs

The package `arboviroses-dengue` has UUID
`4d5e5d44-58a8-4d67-b8aa-4ef1e4b00a1c` — **on both**:

- `pysus/api/dadosgov/databases.py:294` (declared for the SINAN class)
- the Next.js data endpoint (`/_next/data/.../dataset/arboviroses-dengue.json`)

This is huge: the two portals are **the same CKAN instance with two
frontends**. The dataset registry in `pysus.api.types` should treat
the UUID as `identity.cross_origin_id` in the `MetadataBag`, so a
`Comparator` lookup in Stage 4 can match Saude records against the
existing DadosGov records of the same dataset for free. The
`IdentityResolver` in the existing `roadmap.md` §Stage 2 only needs to
be told that **UUID equality = same logical dataset**.

The 14 groups (confirmed live on 2026-08):

```
arboviroses                       → Arboviroses
assistencia-a-saude               → Assistência à saúde
assistencia-farmaceutica          → Assistência Farmacêutica
atencao-primaria                  → Atenção Primária
ciencia-tecnologia                → Ciência & Tecnologia
diagnosticos-e-tratamentos        → Diagnósticos e Tratamentos
economia-da-saude                 → Economia da Saúde
educacao-em-saude                 → Educação em Saúde
indicadores-de-saude              → Indicadores de saúde      ← not in DEMAS Swagger tags
prevencao-e-promocao-da-saude     → Prevenção e Promoção da Saúde
pda                               → Saúde Digital
saude-indigena                    → Saúde Indígena
vacinacao                         → Vacinação
vigilancia-e-meio-ambiente        → Vigilância e Meio Ambiente
```

The 14 groups and the 17 DEMAS Swagger tags are **not the same set**:
two groups (`indicadores-de-saude`, `pda`, `diagnosticos-e-tratamentos`)
have no DEMAS REST endpoint, and three DEMAS tags (`OutrosTemas`,
`Ouvidoria`, `SISVAN`) have no Next.js group. Both should be exposed
under the same `pysus/api/saude/databases.py` registry — datasets that
appear only in one surface are still first-class.

#### 0.5.5 Resource URL pattern

`arboviroses-dengue` resources point to:
`https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN/Dengue/<file>`

i.e. **the Saude resources are hosted on S3 at the same domain as the
DATASUS file repository**. This means the `download_dataset()` helper
from `epidatasets` will, when pointed at this URL, fetch a real file
(some CSV.ZIPs, some PDFs, some API links that should be skipped). The
existing `ExtensionFactory` in `pysus/api/extensions.py:1140-1232`
already handles the resulting ZIP→CSV→Parquet pipeline — the Saude
download path can reuse it without any new code.

#### 0.5.6 Concrete changes to the stages

The findings above update the earlier stages as follows:

- **Stage 1** — `pysus/api/saude/catalog.py` is now a Next.js data
  fetcher (with buildId cache + per-URL TTL cache + retries), not an
  HTML scraper. The 17 dataset classes in
  `pysus/api/saude/databases.py` are extended with a `ckan_slug: str`
  attribute, and Stage 1 wires `fetch_dataset(slug)` →
  `SaudeDatasetExtractor.extract(package_dict)` so the dataset registry
  gets both the DEMAS endpoint list and the CKAN metadata.
- **Stage 2** — `SaudeDatasetExtractor.extract()` populates the entire
  `description`, `provenance`, `temporal`, `access` facets from the
  CKAN package (no more "manual YAML for description" — the portal
  itself is the source).
- **Stage 3** — resource `MetadataBag` is now derived from the rich
  resource dict (19 fields) instead of a `?limit=1` sample row. The
  sample-driven column inference is still needed for the column list
  (DEMAS has no schema), but most other facets come from CKAN.
- **Stage 4** — the comparator uses `identity.cross_origin_id` (the
  shared CKAN UUID) as a **fast equality signal** between Saude and
  DadosGov artifacts of the same dataset. This avoids the current
  need for fragile stem matching across portals.
- **Stage 5** — `download_dataset(slug)` (async port of the epidatasets
  helper) becomes the source for the bulk CSV.ZIP files; the existing
  ZIP→Parquet path through `ExtensionFactory` produces the parquet
  artifact unchanged.

The reusable patterns (`fetch_json` with cache + retries, `_filename_for`,
`_NEXT_DATA_RE` extractor) are documented in §3.1 below as concrete
PySUS idioms.

---

## 1. Metadata architecture (the new foundation)

### 1.1 Current state — metadata is a placeholder

`pysus/api/metadata/` exists but is skeletal:

| File | What's there | What's missing |
|---|---|---|
| `__init__.py` | empty | public API |
| `models.py` | `Dataset`, `DatasetGroup`, `FileMeta`, `File`, `Column` dataclasses (≤ 9 fields each) | no schema, no facets, no validation, no origin-tag |
| `report.py` | empty stub classes `Header`, `Columns`, `Footer` | no real structure, unused outside the test file |

Worse, metadata today is **scattered and inconsistent**:

- FTP extracts year/month/state/group from filenames via per-dataset
  regex in `pysus/api/ftp/databases.py:73-815` (each `formatter()`).
- DadosGov stores a free-form `_metadata: dict` on each `File`
  (`pysus/api/dadosgov/models.py:59-72, 142-172`) and exposes
  `.year` / `.month` / `.state` properties that read from it.
- DuckLake reads pre-existing metadata from the catalog duckdb
  (`pysus/api/ducklake/catalog/orm/columns.py`).
- CKAN catalog descriptions exist but are never imported.

### 1.2 Goals for the metadata layer

1. **One type** — `MetadataBag` — is the canonical representation.
2. **One protocol** — `MetadataExtractor` — that every client implements.
3. **Same surface everywhere** — `BaseRemoteClient`, `BaseRemoteDataset`,
   `BaseRemoteGroup`, `BaseRemoteFile`, `BaseLocalFile` all expose a
   `.metadata` property returning a `MetadataBag`.
4. **Faceted** — metadata is organised into typed facets
   (identity, temporal, spatial, provenance, structure, access, quality),
   not a single flat dict.
5. **Mergeable across origins** — given the same logical entity from
   two clients, merge their bags with a documented precedence per facet.
6. **JSON-serialisable** so it round-trips through the existing DuckLake
   catalogs (`catalog.duckdb`, `catalog_columns.duckdb`,
   `catalog_<name>.duckdb`) and the snapshot JSONs in
   `pysus/management/inventory.py:194-204`.

### 1.3 The `MetadataBag` shape

```
MetadataBag
├── identity      : IdentityFacet     # name, slug, aliases
├── description   : DescriptionFacet  # title, long_name, description, tags
├── temporal      : TemporalFacet     # created, modified, periodicity,
│                                       valid_from, valid_to
├── spatial       : SpatialFacet      # geographic_scope (national|state|regional|
│                                       municipal), uf list, municipality_ibge list
├── provenance    : ProvenanceFacet   # origin, organization, maintainer, contact,
│                                       license, attribution, source_url
├── structure     : StructureFacet    # columns: list[Column], row_count,
│                                       file_count, format, schema_fingerprint
├── access        : AccessFacet       # url, format, size_bytes,
│                                       download_strategy, requires_auth
├── quality       : QualityFacet      # freshness_score, integrity_verified,
│                                       content_fingerprint, completeness_pct
└── raw           : dict[str, Any]    # unmapped fields preserved for round-trip
```

Each facet is a small dataclass with `to_dict()` / `from_dict()`. The
bag itself is:

```python
class MetadataBag(BaseModel):
    identity: IdentityFacet
    description: DescriptionFacet
    temporal: TemporalFacet
    spatial: SpatialFacet
    provenance: ProvenanceFacet
    structure: StructureFacet
    access: AccessFacet
    quality: QualityFacet
    raw: dict[str, Any] = Field(default_factory=dict)

    def merge(self, other: "MetadataBag",
              precedence: Mapping[type, Origin] | None = None) -> "MetadataBag":
        """Field-wise merge with per-facet origin precedence."""
    def to_dict(self) -> dict: ...
    @classmethod
    def from_dict(cls, data: dict) -> "MetadataBag": ...
```

`merge()` resolves conflicts facet by facet using `precedence`
(default: `Saude > DadosGov > FTP > DuckLake` for descriptive fields,
`DuckLake > FTP > DadosGov > Saude` for content/quality fields — see
the decision table in §1.7).

### 1.4 The `MetadataExtractor` protocol

```python
class MetadataExtractor(ABC):
    """Stateless transformer: raw client data → MetadataBag.

    One implementation per (origin, entity_type) pair. Each client
    (`FTP`, `DadosGov`, `Saude`, `DuckLake`) ships its own set of
    extractors. The base classes (`BaseRemoteFile`, etc.) only know
    that there is *some* extractor; they never hard-code which one.
    """

    @abstractmethod
    def extract(self, raw: Any, **ctx) -> MetadataBag: ...

    @abstractmethod
    def supported_facets(self) -> set[str]:
        """Return the subset of facets this extractor can populate."""
```

Concretely, every client ships:

| Client | File-level | Group-level | Dataset-level |
|---|---|---|---|
| **FTP** (`pysus/api/ftp/metadata.py`) | `FtpFileExtractor` (LIST line + per-dataset `formatter()`) | `FtpGroupExtractor` | `FtpDatasetExtractor` |
| **DadosGov** (`pysus/api/dadosgov/metadata.py`) | `DadosGovFileExtractor` (Recurso + filename `formatter()`) | `DadosGovGroupExtractor` | `DadosGovDatasetExtractor` |
| **DuckLake** (`pysus/api/ducklake/metadata.py`) | `DuckLakeFileExtractor` (read from `pysus.files` + `pysus.dataset_columns`) | `DuckLakeGroupExtractor` | `DuckLakeDatasetExtractor` |
| **Saude** (`pysus/api/saude/metadata.py`) | `SaudeFileExtractor` (DEMAS Swagger path + sample row → columns) | `SaudeGroupExtractor` (CKAN resource list) | `SaudeDatasetExtractor` (CKAN dataset page) |

Each extractor declares which facets it can fill
(`supported_facets()`). For FTP, `StructureFacet` is populated only
**after** the file is downloaded and the `.dbc`/`.dbf` schema is read —
so the extractor has an async variant `aextract()` for that case (see
§1.6).

### 1.5 Wiring into the base classes

In `pysus/api/models.py`:

- Add `BaseRemoteClient.metadata_schema_version: ClassVar[int] = 1` and a
  `BaseRemoteClient.metadata_bag() -> MetadataBag` default that returns
  the client-level bag (org, license, contact info).
- `BaseRemoteDataset` and `BaseRemoteGroup` get a `_extractors:
  list[MetadataExtractor]` slot populated by their concrete subclass
  and a `metadata` cached property that calls every extractor in order
  and merges the bags.
- `BaseRemoteFile.metadata` does the same, lazily, with a per-file
  cache (`functools.cached_property`-equivalent — implement via
  `PrivateAttr` like the existing `_metadata` in
  `pysus/api/dadosgov/models.py:59`).
- `BaseLocalFile.metadata` is computed from the local file's content:
  for `Parquet`/`CSV`/`DBF`/`DBC`/`JSON`/`JSONL` it extracts columns
  via the same `_map_dtype` mapping already in
  `pysus/api/extensions.py:34-67`, plus row count and schema
  fingerprint.

Every concrete class picks its extractors in its constructor:

```python
# pysus/api/saude/models.py
class SaudeFile(BaseRemoteFile):
    def __init__(self, **data):
        super().__init__(**data)
        self._extractors = [SaudeFileExtractor()]

# pysus/api/dadosgov/models.py  (refactor, no behaviour change)
class File(BaseRemoteFile):
    def __init__(self, **data):
        metadata = data.pop("_metadata", {})
        super().__init__(**data)
        self._extractors = [DadosGovFileExtractor()]
        self._cached_metadata = metadata  # legacy path
```

### 1.6 Async / sync split

Some facets need network or local file IO (structure from parquet,
freshness from a HEAD request, license from CKAN HTML). The protocol
supports both:

```python
class MetadataExtractor(ABC):
    def extract(self, raw: Any) -> MetadataBag:
        """Synchronous fast path — uses cached/raw info only."""
        return self._extract_sync(raw)

    async def aextract(self, raw: Any) -> MetadataBag:
        """Async path — may hit the network or read local files."""
        return await self._extract_async(raw)
```

`BaseRemoteFile.metadata` exposes only the sync result by default;
calling `await file.ametadata()` triggers the async enrichments
(structure, quality, freshness) and caches the result for subsequent
`.metadata` calls.

### 1.7 Cross-origin merge precedence

| Facet | Highest wins | Rationale |
|---|---|---|
| `identity.name` | first non-empty | all clients agree |
| `description.title/long_name/description` | **Saude > DadosGov > FTP > DuckLake** | Saude is the curated source |
| `temporal.modified` | **DuckLake > FTP > DadosGov > Saude** | S3 timestamp is upload time, accurate |
| `temporal.created` | earliest non-empty | provenance |
| `spatial.geographic_scope` | most specific (municipal > state > national) | never widen coverage |
| `provenance.organization` | first non-empty | |
| `provenance.license` | most permissive (CC-BY > CC-BY-SA > Custom) | |
| `structure.columns` | **DuckLake > Saude > DadosGov > FTP** | DuckLake is authoritative (already-cast parquet schema); Saude sample is inferred |
| `structure.row_count` | largest | DuckLake stores the actual parquet count |
| `access.url` | the originating client (per-origin) | keep both, mark the primary |
| `access.size_bytes` | the client that just fetched (write-time) | S3 knows |

`MetadataBag.merge()` takes a `precedence: Mapping[type, Origin]` arg so
the table can be overridden by configuration. `SyncEngine` calls it
when collapsing artifacts of the same logical file from multiple origins.

### 1.8 Persistence in DuckLake

The DuckLake catalogs must grow three new tables in the `pysus` schema,
mirroring the facet layout:

```sql
CREATE TABLE pysus.file_metadata (
    file_id      INTEGER PRIMARY KEY REFERENCES pysus.files(id),
    identity     JSON,
    description  JSON,
    temporal     JSON,
    spatial      JSON,
    provenance   JSON,
    structure    JSON,
    access       JSON,
    quality      JSON,
    raw          JSON,
    extracted_at TIMESTAMP
);

CREATE TABLE pysus.group_metadata (...);
CREATE TABLE pysus.dataset_metadata (...);
```

(JSON columns are DuckDB `JSON` type — already supported via the
`duckdb` Python driver; no migration risk.)

The new tables are populated by `CatalogWriter` (extending
`pysus/management/catalog.py:67+`) in the same transaction that writes
the file row today. The existing `pysus.dataset_columns` table
(`pysus/api/ducklake/catalog/orm/columns.py:9-23`) stays — it is the
high-traffic subset of `structure.columns` that powers the most common
query path.

### 1.9 Tests for the metadata layer

- `pysus/tests/api/metadata/test_bag.py` — round-trip + merge tests for
  every facet; precedence table validated.
- `pysus/tests/api/metadata/test_extractors.py` — one fixture per
  (origin, entity_type) → `(extractor, raw_input, expected_bag)`.
- `pysus/tests/api/metadata/test_base_classes.py` — assert that every
  `BaseRemote*` class surfaces `.metadata` and that the merge across
  origins works on synthetic `CNES` data from FTP + DadosGov + Saude.

### 1.10 Metadata layered into the Saude stages

Every Saude-related stage below (Stage 1–6) now has an explicit
metadata deliverable:

- **Stage 1** — `pysus/api/saude/metadata.py` with the three
  `SaudeDatasetExtractor` / `SaudeGroupExtractor` / `SaudeFileExtractor`
  classes. Stage 1 also adds the abstract base classes
  (`MetadataBag`, `MetadataExtractor`, all facets) to
  `pysus/api/metadata/` so FTP/DadosGov/DuckLake can adopt them.
- **Stage 2** — `SaudeDatasetExtractor.extract()` populates
  `description`, `provenance` (organization, license, attribution),
  `spatial` (national scope), `temporal` (created/modified from CKAN
  page), `identity` (slug + aliases). The 17 dataset classes get a
  `metadata_overrides: MetadataBag | None` class attribute that the
  Saude extractor merges with the live CKAN data.
- **Stage 3** — `SaudeFileExtractor.extract()` populates `access`
  (URL, format), `structure` (columns inferred from a `?limit=1`
  sample), `temporal` (created/modified from the resource list). The
  async path `aextract()` upgrades `quality.freshness_score` from a
  live HEAD request.
- **Stage 4** — `Inventory._collect_saude()` stores the full
  `MetadataBag.to_dict()` on each `FileRecord` (extend the dataclass
  in `pysus/management/records.py:174-202` with a `metadata: dict |
  None` field).
- **Stage 5** — `SyncEngine.upload_file()` accepts a `File` whose
  `.metadata` populates `pysus.file_metadata` in the same transaction
  as the file row (extend `CatalogWriter` in
  `pysus/management/catalog.py`).
- **Stage 6** — `DuckDataset.metadata` and `DuckFile.metadata` use the
  DuckLake extractors (read from the new tables) and the abstract base
  classes; the front-end `pysus/web/pages/*.py` can then show the
  CKAN-derived description, license, periodicity for Saude-sourced
  datasets without code changes.

This makes metadata a **first-class deliverable** of every stage, not
a footnote.

---

## 2. Architectural decision: a new origin (metadata-aware)

Insert `"saude"` alongside `"ducklake" | "ftp" | "dadosgov"` everywhere
those strings appear as an enum-like constant. Concretely:

| Layer | Current tuple | New tuple |
|---|---|---|
| `pysus/api/types.py` `_validate_origin` | `("FTP", "DadosGov", "DuckLake")` | `("FTP", "DadosGov", "DuckLake", "Saude")` |
| `pysus/management/records.py` `ORIGINS` | `("ducklake", "ftp", "dadosgov")` | `("ducklake", "ftp", "dadosgov", "saude")` |
| `pysus/management/inventory.py` `_ORIGIN_TO_CLIENT` | `{"ftp": "ftp", "dadosgov": "dadosgov", "ducklake": "ducklake"}` | add `"saude": "saude"` |
| `pysus/api/client.py` `PySUS._dadosgov` field | only DadosGovClient | add `_saude: SaudeClient | None` and `get_saude()` lazy accessor |
| `pysus/management/sync.py` `SyncEngine.__init__` / `run()` | `dadosgov_token` arg | add `saude_token` (optional, no-op if unused — public read API) |

`DownloadPriority` stays `S3 → FTP → DadosGov` because:

- Saude endpoints do not produce downloadable files in the same sense
  (rows are returned as JSON, not written to S3 as parquet artifacts
  derived from a file).
- S3 is still preferred over anything, including Saude, when a parquet
  exists.
- When a logical file is **only** on Saude, the engine will need to
  either (a) fall back to paginated JSON, or (b) stage the rows through
  the local parquet builder without writing to S3. See §5.4.

---

## 3. New module layout

```
pysus/api/saude/
    __init__.py           # exports SaudeClient
    client.py             # SaudeClient(BaseRemoteClient)
    catalog.py            # CKAN HTML scraper (lxml + httpx)
    rest.py               # DEMAS REST client (paged fetch helpers)
    models.py             # Recurso (resource), ConjuntoDados (dataset),
                          # Dataset/Group/File wrappers
    databases.py          # AVAILABLE_DATABASES for health datasets
                          # (replaces/dedupes part of dadosgov/databases.py)
    metadata.py           # column/endpoint metadata discovery
    schemas.py            # cached column descriptions (manual + sample-driven)
pysus/tests/api/saude/
    test_client.py
    test_catalog.py
    test_rest.py
    test_models.py
    test_databases.py
```

Mirror the shape of `pysus/api/dadosgov/` exactly so existing patterns
(BaseRemoteClient, `Group._fetch_files`, `File.fetch_metadata`, the
`formatter()` convention) transfer with minimal friction.

---

## 4. Stage-by-stage plan

The rollout has **6 stages**, each independently shippable behind a
feature flag, each with explicit pass criteria.

### Stage 1 — Skeleton client and discovery

**Goal:** `DadosAbertosSaude` can be instantiated, connected without a
token, and `datasets()` returns 17 pre-configured health datasets.

Tasks:

1. **Create `pysus/api/types.py` enum extension**
   - Add `SAUDE: Annotated[str, AfterValidator(_validate_origin)] = "Saude"` and extend `_validate_origin`.
   - Add a new `DatasetName` validator entry for each new health dataset that does **not** exist today (PNI/SINAN/SIM/SINASC/CNES are reused). New names needed: `BNAFAR`, `SISAGUA`, `SISVAN`, `SIASI`, `SESAI`, `PMMB`, `PREVINEPB`, `BPS`, `CONITEC`, `PLATAFORMABR`, `OUVIDORIA`, `MACROSAUDE`, `EPI`, `ENANI`, `PVC`, `CED`.
   - `pysus/api/types.py:21-24` (current `_validate_origin`), `pysus/api/types.py:59-72` (`_validate_dataset_name`).

2. **Implement `pysus/api/saude/client.py`**
   - `class SaudeClient(BaseRemoteClient)`
     - `base_url = "https://apidadosabertos.saude.gov.br"`
     - `_catalog_url = "https://dadosabertos.saude.gov.br"`
     - `_swagger_url = "{base_url}/static/swagger.json"`
   - `connect()` is a no-op (no token) — set up an `httpx.AsyncClient` and the cached Swagger spec; existing
     `connect(token=...)` signatures should accept and ignore `token`.
   - `long_name = "Portal de Dados Abertos do SUS"`
   - `description = "Interface de acesso ao Portal de Dados Abertos do Ministério da Saúde"`

3. **Implement `pysus/api/saude/catalog.py` — CKAN HTML scraper**
   - `async def list_dataset_slugs() -> list[str]` — paginates
     `https://dadosabertos.saude.gov.br/dataset?rows=...&page=N`, parses
     `<a href="/dataset/<slug>">` links. Reuses the structure used in
     `pysus/api/ftp/models.Directory` for navigation.
   - `async def fetch_dataset(slug: str) -> ConjuntoDados` — fetches
     `https://dadosabertos.saude.gov.br/dataset/<slug>`, parses
     `<h1>`, `<div class="notes">`, resource list (`<a class="resource-url">`),
     tags, license, organization.
   - **Discovery uses HTML only** (no Action API). Cache aggressively
     (`CACHEPATH / "saude" / "catalog.json"`) — refresh TTL 6 h.
   - Note: CKAN page structure (theme/group sections like "Arboviroses",
     "Assistência à Saúde") is **not** a separate API; parse them from
     the index page when building the dataset registry.

4. **Implement `pysus/api/saude/rest.py` — DEMAS REST helper**
   - `async def fetch_swagger() -> dict` — fetch `https://apidadosabertos.saude.gov.br/static/swagger.json`, cache under
     `CACHEPATH / "saude" / "swagger.json"` with a TTL. Validate against
     a pinned hash so a server-side schema change triggers a refresh.
   - `async def query(endpoint_path: str, params: dict) -> AsyncIterator[dict]` — yields paginated rows by walking `offset` until
     the response is empty, respecting the documented `limit ≤ 1000`
     ceiling per endpoint.
   - `async def fetch_all(endpoint_path: str, params: dict | None = None) -> list[dict]` — convenience wrapper.

5. **Wire into `PySUS`** (`pysus/api/client.py:99-135`)
   - Add `_saude: SaudeClient | None = None`.
   - Add `async def get_saude(self) -> SaudeClient` mirroring
     `get_dadosgov`.
   - Register `saude_client` in `__aexit__` close list
     (`pysus/api/client.py:108-117`).

6. **Add to `pysus/api/__init__.py`** if a re-export is desired (mirror
   the existing `from .dadosgov import DadosGov as DadosGovClient` line
   in `pysus/api/dadosgov/__init__.py:1-3`).

7. **Tests (`pysus/tests/api/saude/test_client.py`)** — using
   `httpx.MockTransport`:
   - Mock the index page; assert `list_dataset_slugs()` returns the
     expected list.
   - Mock the DEMAS Swagger JSON; assert `fetch_swagger()` parses paths
     and tags correctly.
   - Assert `connect()` accepts a missing token (no auth header required).

Pass criteria:

- `async with SaudeClient() as c: ds = await c.datasets()` works offline
  against mocks.
- Cached catalog refreshes when the cached file is older than 6 h.
- All existing tests still pass.

### Stage 2 — Dataset registry (`AVAILABLE_DATABASES`)

**Goal:** every Saude health dataset can be enumerated the same way
`CNES`, `PNI` etc. are today in `pysus/api/dadosgov/databases.py`.

Tasks:

1. **Define `pysus/api/saude/databases.py`** mirroring the structure of
   `pysus/api/dadosgov/databases.py` (the `Dataset` ABC with
   `formatter()`, `ids`, `group_aliases`).
   - 17 base classes, one per Swagger tag (see `apidadosabertos.saude.gov.br/static/swagger.json` "tags"):
     `Arboviroses`, `AssistenciaSaude`, `AtencaoPrimaria`, `BNAFAR`,
     `CNES`, `CienciaTecnologia`, `EconomiaSaude`, `EducacaoSaude`,
     `MacroSaude`, `OutrosTemas`, `Ouvidoria`, `PrevencaoPromocao`,
     `SISAGUA`, `SISVAN`, `SaudeIndigena`, `Vacinacao`, `VigilanciaMeioAmbiente`.
   - Each subclass populates `endpoints: list[str]` (the DEMAS paths
     under that tag) plus the CKAN `slug` for metadata scraping. Each
     endpoint becomes one `File` whose `record.api_size` is the row
     count returned by `?limit=1`, and whose `path` is the full DEMAS URL.
   - For datasets that also exist on `dados.gov.br` (CNES, PNI, SIM,
     SINASC, Arboviroses), the Saude-side `Dataset` declares a
     `alias_of: str | None = "dadosgov"` so `SyncEngine` can collapse the
     two origins into a single logical file (see Stage 4). Today those
     datasets are declared in `pysus/api/dadosgov/databases.py:40-624`:
     `CNES`, `PNI`, `SIA`, `SINAN`, `SIM`, `SINASC`, `COVID19`. New
     datasets do not collide.

2. **Migrate overlapping datasets** — decision needed:
   - **Option A (recommended):** keep the DadosGov declarations but add
     Saude as a *secondary* origin; both classes implement the same
     `formatter()` keys; `SyncEngine` records both into the catalog.
   - **Option B:** move CNES/PNI/SIM/SINASC/Arboviroses entirely into
     `pysus/api/saude/databases.py` and leave only "DadosGov-only"
     datasets (SIA, COVID19) in `pysus/api/dadosgov/databases.py`. Less
     duplication but breaks the file the rest of the codebase
     references.

3. **Extend `formatter()` to accept endpoint-derived metadata.** Current
   `formatter()` in `pysus/api/dadosgov/databases.py:85-114` operates on
   filename strings. For Saude, the `name` will be the endpoint path
   (e.g. `vacinacao/doses-aplicadas-pni-2024`) and the metadata comes
   from:
   - year: parsed from the path (`/doses-aplicadas-pni-2024` → 2024) or
     the query payload's `nu_ano` field;
   - month: only when the endpoint returns month-resolved rows (none
     currently does);
   - state/UF: never directly — left `None` and resolved at query time
     via `codigo_uf` filter.

4. **Tests (`pysus/tests/api/saude/test_databases.py`)** — one test per
   dataset that constructs the class and asserts the canonical name,
   long name, description, and a smoke-test `formatter()` on a
   representative endpoint path.

Pass criteria: `SaudeClient.datasets()` returns 17 classes, each
exercisable.

### Stage 3 — REST query path and the `File` model

**Goal:** a user can fetch paginated rows from any Saude endpoint as
`BaseRemoteFile` objects, with uniform `.year`, `.size`, `.modify`,
`.download()` semantics.

Tasks:

1. **Implement `pysus/api/saude/models.py`** — `Recurso`, `ConjuntoDados`,
   `File`, `Group`, `Dataset` mirroring
   `pysus/api/dadosgov/client.py:300-345` and
   `pysus/api/dadosgov/models.py:54-348`:
   - `File._download(output)` does **not** download a CSV/ZIP — it
     writes the paginated JSON to `output` as a single JSON Lines file
     (one JSON object per row). Add a new `FileType` literal
     `JSONL = "JSONL"` to `pysus/api/types.py:_validate_file_type`
     (line 41-56) and the matching enum literal (line 137-145).
   - `File._download_json_pages()` (new) — async generator yielding
     `(row, total)` and tracking `progress`.
   - `File.size` is the *byte size of the JSONL artifact* (computed on
     first download — store as `api_size` after the first write).
   - `File.modify` is the swagger `info.version` (5.32.12), bumped by the
     server when the schema changes — a stable freshness signal since
     Saude has no per-row modification timestamps.

2. **Add a `JSONL` extension** to `pysus/api/extensions.py` (a sibling of
   the existing `JSON` class at line 727) so the resulting artifact can
   be loaded as a `pd.DataFrame` and `to_parquet()` works. Reuse the
   `_map_dtype()` mapping at `pysus/api/extensions.py:34-58`.

3. **Map the Swagger response columns to the catalog**:
   - Each `File` exposes `.columns` (computed by sampling `?limit=1` on
     the endpoint). Cache the response in
     `CACHEPATH / "saude" / "schema" / "<endpoint>.json"` with TTL 24 h.
   - The mapping `{column_name: arrow_type}` is derived from the JSON
     types of the sample row (string → VARCHAR, integer → INTEGER,
     float → DOUBLE, bool → BOOLEAN, ISO date → DATE — extend
     `_map_dtype()` in `pysus/api/extensions.py:34-67` accordingly).

4. **`SaudeClient.download()`** in `pysus/api/saude/client.py`:
   - Stream pages with `httpx.AsyncClient.stream("GET", url)` (mirror
     `pysus/api/dadosgov/client.py:269-297`).
   - Default `limit=1000` per page; `offset` increments by the page size.
   - Stop when the response body returns `[]` or fewer than `limit` rows.

5. **Tests (`pysus/tests/api/saude/test_rest.py`, `test_models.py`)** —
   - Mock the DEMAS endpoint to return 3 pages of varying lengths.
   - Assert the JSONL artifact is well-formed, has the expected row
     count, and `.to_parquet()` produces a valid parquet.

Pass criteria:

- `await file.download()` writes a JSONL file loadable as a DataFrame.
- Endpoint changes that bump `info.version` cause a re-download.

### Stage 4 — Inventory + compare integration

**Goal:** Saude datasets flow through the same `inventory → compare →
sync` pipeline as the existing origins.

Tasks:

1. **Extend `pysus/management/inventory.py:_ORIGIN_TO_CLIENT`** (line
   26-30) with `"saude": "saude"`.

2. **Add `Inventory._collect_saude()`** in
   `pysus/management/inventory.py` after `_collect_dadosgov` (line
   123-154). It calls `await self.pysus.get_saude()`, iterates
   `datasets()`, and produces `FileRecord` objects where:
   - `origin = "saude"`,
   - `name = endpoint_path` (e.g. `doses-aplicadas-pni-2024`),
   - `path = https://apidadosabertos.saude.gov.br/<endpoint>`,
   - `size = File.api_size` after a sample fetch,
   - `modified = File.modify` (Swagger version),
   - `year/month/state/group` parsed by the dataset's `formatter()`.

3. **Update `Inventory.collect` and `collect_all`** (line 44-74) to
   dispatch `"saude"`.

4. **Extend `FileRecord` schema** — `management/records.py:174-202`
   already accepts arbitrary fields, but the JSON snapshot schema needs
   `format="jsonl"` for Saude entries. Update
   `records.py:_format_of()` (line 136-148) to recognise `.jsonl`.

5. **Comparator cross-format logic**
   (`pysus/management/compare.py`):
   - The existing `format_dedup` prefers CSV; for Saude entries there
     is no equivalent triplet to dedup. Confirm it remains a no-op for
     `format="jsonl"`.
   - **Cross-origin identity:** a Saude `doses-aplicadas-pni-2024`
     (year=2024) should map to the same `IdentityKey` as the existing
     DadosGov `doses-aplicadas-pni-2024_csv.zip`. Confirm
     `stem_of("doses-aplicadas-pni-2024")` → `"doses_aplicadas_pni_2024"`
     equals the stem derived from the zip filename. If not, add an
     override map in `management/compare.py`.

6. **Snapshot persistence** — `save_snapshot`/`load_snapshot` already
   round-trip arbitrary fields. Add `saude.json` to the snapshot set
   in `management/scripts/compare_clients.py` if it iterates over
   origins explicitly.

7. **Tests (`pysus/tests/management/test_inventory.py`)** — extend the
   existing parametrized fixtures with a Saude fixture. Snapshot
   round-trip must be byte-identical.

Pass criteria: `Inventory.collect_all()` returns four lists with the
same schema; `Comparator.compare()` correctly groups Saude and DadosGov
records that share metadata.

### Stage 5 — Sync engine integration

**Goal:** Saude endpoints become first-class sync targets, with
idempotent, resumable ingestion.

Tasks:

1. **Extend `SyncEngine.upload_file()`** in
   `pysus/management/sync.py:212-366` to accept a `SaudeFile`:
   - `saude_file._download(output)` writes the JSONL artifact
     (downloaded once into `tmp` via `_download_raw_with_retry`,
     line 384-418).
   - The pipeline becomes **download → convert JSONL → parquet**:
     - Add `pysus/api/extensions.py:JSONL.to_parquet()` (chunked read of
       JSONL → `pd.DataFrame` → `pq.ParquetWriter`; mirror
       `extensions.py:259-372` for `Parquet`).
     - Wire it through `ExtensionFactory.instantiate()` (line 1181) via
       the `.jsonl` extension entry.
   - The content-veto, size check, and hash logic at
     `sync.py:269-298` work as-is because they operate on raw bytes.

2. **S3 key layout** — extend `compose_s3_key()` in
   `management/records.py:100-133` to accept `origin="saude"` (already
   a free-form string; no code change). Add a `_saude_origin_aliases`
   helper that maps Saude endpoint paths to the dataset canonical name
   (e.g. `doses-aplicadas-pni-2024` → `PNI`).

3. **Download priority** — keep the existing `S3 → FTP → DadosGov`
   order. Saude entries that have a corresponding FTP/DadosGov artifact
   never become the download source. Add Saude as a 4th origin in
   `_pick_source()` at `sync.py:1148-1156` only when no other source has
   the file:
   - `_pick("saude")` becomes the fallback after FTP and DadosGov.
   - This means **Saude-only files** (e.g. `BNAFAR`,
     `SISAGUA/vigilancia-parametros-basicos`, `SaudeIndigena/*`) are
     ingested through the same pipeline.

4. **`_preconnect_adapters`** (`sync.py:935-949`) — the dataset adapter
   set is keyed on `r.dataset.lower()`. Saude records must declare
   the canonical dataset name (e.g. `PNI`, `BNAFAR`, `SISAGUA`) so
   their files land in the right per-dataset catalog file. Verify the
   filter `r.origin in ("ftp", "dadosgov")` (line 946) — change to
   `r.origin in ("ftp", "dadosgov", "saude")`.

5. **Schema migration for Saude parquet**
   (`pysus/management/catalog.py:CatalogWriter`):
   - `link_columns()` (referenced at `sync.py:927-933`) uses
     `payload["schema"]` returned by `Parquet.schema` (in
     `extensions.py:270-274`). The Saude parquet will have its columns
     derived from the JSONL sample — populate the column descriptions
     table (`catalog_columns.duckdb`) with `description=""` initially
     and document a manual review path (see Stage 7).

6. **`PySUS.download_to_parquet()` path**
   (`pysus/api/client.py:346-414`) — the `client_name` switch at line
   289-301 must add `elif client_name == "saude": client = await self.get_saude()`.

7. **CLI** — `pysus/management/scripts/sync_clients.py:33-60` should
   forward a `--saude-only` flag and pass `saude_token` (optional, no-op)
   to `SyncEngine(...)`. Mirror the `--datasets` filter behavior for
   Saude.

8. **Tests (`pysus/tests/management/test_sync.py`)** — new parametrized
   test cases for Saude files: one with a same-day fresh file on S3
   (skipped), one with size-mismatched FTP copy (reprocessed), one
   Saude-only (ingested via JSONL → parquet).

Pass criteria: `python -m pysus.management.scripts.sync_clients --datasets PNI`
ingests `doses-aplicadas-pni-2024` from Saude when the FTP/DadosGov
copy is stale, and writes a parquet at
`public/data/saude/pni/2024/_/_/_/doses_aplicadas_pni_2024.parquet`.

### Stage 6 — DuckLake (catalog) integration

**Goal:** DuckLake can discover, query, and inspect Saude-sourced
parquets the same way it inspects FTP/DadosGov-sourced ones.

Tasks:

1. **New per-dataset catalogs.** Each Saude dataset (`BNAFAR`,
   `SISAGUA`, `SISVAN`, etc.) gets a `catalog_<name>.duckdb` in the S3
   bucket at `public/catalog_<name>.duckdb`. The `DatasetAdapter`
   constructor in `pysus/api/ducklake/catalog/adapters.py:382-388`
   already accepts any name; no code change, but a `populate` step is
   needed on first upload (existing `catalog.duckdb` rows reference
   `dataset_id` from `pysus.datasets` — the dataset registry row must be
   inserted via `CatalogWriter.ensure_dataset()` at
   `management/catalog.py:118+`).

2. **Register the new dataset names** in `pysus.api.types` (Stage 1.1
   extended) so `_validate_dataset_name` accepts `BNAFAR`, `SISAGUA`,
   etc. The `CatalogWriter.upsert_file()` call at
   `sync.py:903-922` reads `file.dataset.name` and writes to
   `pysus.datasets` through `ensure_dataset()` — both already
   schema-driven, but the `pysus.ducklake` reader in
   `pysus/api/ducklake/models.py:142-191` filters by `dataset_id`
   only, so the dataset **must** be registered first.

3. **Column metadata backfill.** Since Saude returns no per-column
   descriptions, take one of three approaches (in order of preference):

   - **(a) Manual YAML** — add
     `pysus/api/saude/schemas/<dataset>.yaml` files that map column
     name → `(dtype, description_pt, description_en)`. Author these
     from the public SUS documentation (the same source used to build
     the cartilha referenced on `dadosabertos.saude.gov.br`). Add a
     loader that reads the YAML on first access and populates
     `dataset_columns` (existing table at
     `pysus/api/ducklake/catalog/orm/columns.py:9-23`).
   - **(b) Sample-driven** — fetch `?limit=1` per endpoint on first
     access, store the inferred `(column, dtype)` into
     `catalog_columns.duckdb` with `description=""`.
   - **(c) Borrow from sibling datasets** — reuse the
     `SINAN-DENG` schema for `arboviroses-dengue`, the
     `PNI-aplicacao` schema for the other PNI years, etc., so a single
     yaml covers ~10 endpoints.

   The recommended default is (b) with manual override via (a). (c) is
   opportunistic.

4. **Extend `DuckLake.datasets()`** (`pysus/api/ducklake/client.py:116-145`).
   It already reads the central catalog and returns everything; no code
   change is needed, but ensure that newly uploaded Saude datasets
   appear in the next list call by re-uploading the central catalog
   after a Saude sync run (existing `mark_dirty()` at
   `adapters.py:80-82` handles this).

5. **`DuckDataset.query()`** (`pysus/api/ducklake/models.py:142-191`)
   filters by `year`, `month`, `state`, `group`. Saude parquets will
   not have a `state` column (national only); confirm the filter
   `CatalogFile.state.in_([s.upper() for s in states])` (line 178)
   correctly returns national files when state is `None`. If not,
   add a fallback: `OR state IS NULL` when state filter is provided.

6. **Frontend** (`pysus/web/pages/*.py`) — add Saude dataset tiles
   alongside the existing ones, mirroring
   `docs/source/guides/dadosgov.rst` with a new
   `docs/source/guides/saude.rst`.

7. **Tests (`pysus/tests/api/ducklake/test_catalog.py`)** — extend
   with a `test_saude_dataset_visible` test that pushes a synthetic
   parquet + catalog row and asserts the dataset appears in
   `DuckLake.datasets()`.

Pass criteria:

- After a sync run that includes BNAFAR, `await DuckLake.datasets()`
  returns a dataset named `BNAFAR` with the expected rows.
- `duckdb.execute("SELECT name, type FROM catalog_columns.duckdb WHERE
  dataset_id = (SELECT id FROM pysus.datasets WHERE name='BNAFAR')")`
  returns one row per column of the JSONL sample.

---

## 5. Risks and mitigations

1. **No response schema in the DEMAS Swagger.** Mitigated by Stage 3.3
   (sample-driven column inference) and Stage 6.3 (manual YAML).

3. **HTML-only CKAN catalog** — fragile to portal redesign. Mitigate
   with robust lxml parsing, a snapshot in `CACHEPATH / "saude"
   / "catalog.json"` and a unit test that asserts the index page
   contains expected slugs (so redesigns are caught at test time, not in
   production).

4. **Tokenless public API** — no rate limit is documented; risk of
   accidental DoS. Add a polite `asyncio.sleep(0.1)` between paginated
   requests and an opt-in `--saude-rate-limit N` (requests/sec) knob.

5. **`info.version` is the only freshness signal.** Document this
   limitation; treat any version bump as "schema may have changed;
   re-download". If the ministry starts serving per-row timestamps in a
   future API version, swap the freshness source.

6. **JSONL → parquet parity with .dbc → parquet.** A SINAN arbovirose
   row fetched from Saude will likely differ in dtype from the FTP
   `.dbc`-derived parquet (e.g. "nan" strings vs NULL). The content
   fingerprint code in `management/compare.py` already stringifies values
   before hashing — confirm the JSONL path uses the same code path.

7. **Auth flow change.** Existing code calls
   `connect(token=...)` for DadosGov and FTP. Saude requires no token.
   Accept the kwarg for symmetry but ignore it; document that the
   header is intentionally absent.

---

## 6. Key decisions to confirm before implementation

- **Origin name.** `saude` (lowercase, matches the existing convention).
- **FileType.** Add `JSONL` to the file-type enum (`types.py:41-56`).
- **Existing dataset registry** — move CNES/PNI/SIM/SINASC/Arboviroses
  into `pysus/api/saude/databases.py`, or keep them in DadosGov with
  Saude as a secondary origin? (Default: keep in DadosGov, add Saude as
  secondary; revisit in Stage 5 if the formatter collisions become
  painful.)
- **Sample-driven column metadata** as the default? (Default: yes, with
  manual YAML override.)
- **Synchronous fetch or async pagination for large endpoints?** The
  SRAG endpoint has millions of rows; default to async pagination with
  `limit=1000`, but allow `--saude-full` (no `limit`) for offline
  ingestion.

---

## 7. Rollout checklist

- [x] Stage 1 — skeleton client + Swagger parse + tests pass
- [x] Stage 2 — 17 datasets exposed, formatter() works on representative endpoints
- [x] Stage 3 — JSONL download works, schema inferred from sample
- [x] Stage 4 — Inventory collector + Comparator cross-origin identity
- [x] Stage 5 — `sync_clients --datasets BNAFAR` writes the first Saude parquet
- [x] Stage 6 — `DuckLake.datasets()` shows BNAFAR; column metadata persisted
- [x] docs/source/guides/saude.rst written (mirror of dadosgov.rst)
- [x] CHANGELOG.md entry per stage

After Stage 6, Saude becomes a first-class origin like FTP and DadosGov:
all `SyncEngine`, `Inventory`, `CatalogWriter`, `DuckLake` machinery
treats it uniformly, no special-case branches.

---

## 8. Touch-point reference (where to look)

| File | Why |
|---|---|
| `pysus/api/types.py:21-24, 41-56, 59-72, 109-145` | Add origin/dataset-name/file-type literals |
| `pysus/api/dadosgov/client.py:67-298` | Pattern reference for new `SaudeClient` |
| `pysus/api/dadosgov/models.py:54-348` | Pattern reference for `File`/`Group`/`Dataset` |
| `pysus/api/dadosgov/databases.py:1-660` | Pattern reference for `AVAILABLE_DATABASES` |
| `pysus/api/extensions.py:34-67, 727-770, 1140-1232` | Add `JSONL` extension + `_map_dtype` entries |
| `pysus/api/client.py:99-135, 269-336` | Register `get_saude()` lazy accessor + download dispatch |
| `pysus/management/inventory.py:26-30, 44-186` | Add `_collect_saude()` + dispatch |
| `pysus/management/compare.py` | Confirm stem/format dedup for `jsonl` |
| `pysus/management/records.py:16-20, 100-202` | Extend ORIGINS + snapshot format |
| `pysus/management/sync.py:212-1156` | Extend `upload_file` + `_pick_source` |
| `pysus/management/catalog.py` | No code change, but verify column write path |
| `pysus/management/scripts/sync_clients.py` | Add `--saude-only` / `--saude-rate-limit` flags |
| `pysus/api/ducklake/client.py:116-145` | No code change; verify central catalog reload after Saude sync |
| `pysus/api/ducklake/models.py:142-191` | Filter tweak for national-only Saude files |
| `pysus/api/ducklake/catalog/orm/columns.py:9-23` | Reused as-is; populate from Stage 6.3 |
| `docs/source/guides/dadosgov.rst` | Mirror for `saude.rst` |
