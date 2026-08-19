# Roadmap Progress: dadosabertos.saude.gov.br integration

Tracks implementation progress against `roadmap_saude.md` and
`roadmap_saude_stage1_spike.md`. Updated after each stage.

| Stage | Description | Status | Notes |
|---|---|---|---|
| 1 | Saude catalog + downloads client (spike) | ✅ complete | see `roadmap_saude_stage1_spike.md` |
| 0 | Metadata architecture (MetadataBag + extractors) | ✅ complete | delivered 2026-08-17 |
| 2 | Dataset registry (`AVAILABLE_DATABASES`) | ✅ complete | delivered 2026-08-17 |
| 3 | DEMAS REST query path + File model | ✅ complete | delivered 2026-08-19 |
| 4 | Inventory + compare integration | ⏳ pending | |
| 5 | Sync engine integration | ⏳ pending | |
| 6 | DuckLake catalog integration | ⏳ pending | |

---

## Stage 1 — Saude catalog client (2026-08-17)

Delivered as a self-contained spike. **53 tests**, offline fixtures
captured from the live portal.

### Files added

| File | Purpose |
|---|---|
| `pysus/api/saude/__init__.py` | public exports (`SaudeClient`, models, errors) |
| `pysus/api/saude/client.py` | async facade owning httpx client + caches |
| `pysus/api/saude/next_data.py` | Next.js buildId extraction + cache |
| `pysus/api/saude/catalog.py` | Next.js data fetcher (listing, groups, tags, packages) |
| `pysus/api/saude/resources.py` | pydantic models: `CKANPackage`, `Resource`, `CatalogEntry`, `CatalogPage`, `Organization`, `Extra`, `GroupRef`, `TagRef` |
| `pysus/api/saude/download.py` | async resource/dataset download helpers |
| `pysus/api/saude/errors.py` | `SaudeError`, `BuildIdMissing`, `PortalChanged`, `DatasetNotFound`, `ResourceNotFound`, `NoUsableBuildId` |
| `pysus/cli/saude.py` | typer sub-app: `list-datasets`, `list-groups`, `show`, `download` |
| `docs/source/guides/saude.rst` | user guide |
| `pysus/tests/api/saude/` | 5 test modules + captured fixtures |

### Files edited

- `pysus/cli/__init__.py` — registered `saude` typer sub-app
- `docs/source/guides/index.rst` — added `saude`
- `CHANGELOG.md` — feature entry under 2.9.0

### Verification

- `pysus/tests/api/saude/` → 53 passed (offline)
- `pysus/tests/api/` → 697 passed
- `pysus/tests/management/` → 143 passed
- `ruff check` clean; `ruff format` applied

---

## Stage 0 — Metadata architecture (complete, 2026-08-17)

Goal: one `MetadataBag` type, one `MetadataExtractor` protocol, `.metadata`
on every base class, faceted layout, mergeable across origins.

### Checklist

- [x] `pysus/api/metadata/models.py` — 8 facets + `MetadataBag` + `merge_bags`
      (per-facet precedence; legacy dataclasses kept for backwards compat)
- [x] `pysus/api/metadata/extractors.py` — `MetadataExtractor` protocol
      (sync `extract` + async `aextract` + `supported_facets`)
- [x] `pysus/api/metadata/__init__.py` — public exports
- [x] Wire `.metadata` / `.ametadata()` into base classes via
      `MetadataMixin` (`pysus/api/models.py`); `BaseRemoteFile` and
      `BaseRemoteObject` both mix it in; `BaseTabularFile.metadata`
      computes structure from local content
- [x] `pysus/api/saude/metadata.py` — Saude dataset/group/file extractors
      (CKAN package → description/provenance/temporal facets incl.
      periodicity, contact, license, cross-origin UUID)
- [x] `pysus/api/dadosgov/metadata.py` — DadosGov extractors
- [x] `pysus/api/ftp/metadata.py` — FTP extractors
- [x] `pysus/api/ducklake/metadata.py` — DuckLake extractors (row count,
      sha256 → quality facet)
- [x] Registered extractors on concrete models via `extractor_types`
      ClassVar (dadosgov/ftp/ducklake `models.py`)
- [x] Tests: `pysus/tests/api/metadata/` — `test_bag.py` (24),
      `test_extractors.py` (15), `test_base_classes.py` (6) — 45 tests
- [x] Docs: `docs/source/guides/metadata.rst`
- [x] CHANGELOG entry (2.9.0, `metadata` feature line)

### Design decisions taken during implementation

- `MetadataMixin` (not pydantic `PrivateAttr`) for the cache — pydantic
  v2 only collects `PrivateAttr` from `BaseModel` bases, and
  `BaseRemoteFile` puts the mixin after `BaseFile`; plain `getattr`/
  `setattr` storage works on any pydantic instance.
- `extractor_types: ClassVar[list]` per concrete class (pydantic
  requires ClassVar annotation for non-field class attributes).
- Merge precedence constants exported from
  `pysus.api.metadata.models`: `DESCRIPTIVE_PRECEDENCE`,
  `STRUCTURE_PRECEDENCE`, `MODIFIED_PRECEDENCE`, `SCOPE_RANK`.

### Verification

- `pysus/tests/api/metadata/` → 45 passed
- `pysus/tests/api/saude/` → 53 passed (still green after base-class
  changes)
- full `pysus/tests/` → 924 passed, 1 pre-existing failure
  (`test_dbf_reader.py::test_numpy_object_array_types` — pandas-version
  dtype assertion, unrelated)
- `ruff check` clean; `ruff format` applied

---

## Stage 2 — Dataset registry (complete, 2026-08-17)

### Deliverables

- `pysus/api/saude/databases.py` — data-driven `DatasetSpec` registry
  with **19 specs** (14 CKAN themes + pattern-based entries for CNES,
  SISAGUA, SISVAN, Ouvidoria, Outros Temas); each spec carries
  `ckan_group`, `slug_patterns`/`exclude_patterns`, DEMAS `tags` and
  `endpoints` (87 total across specs); `spec_for(slug, groups)` +
  `parse_year(name)` helpers
- `pysus/api/saude/models.py` — `SaudeDataset` (spec-based theme),
  `SaudeGroup` (one CKAN package, lazy package fetch), `SaudeFile`
  (one resource; `.year` parsed from name; `_download()` streams the
  resource); extractors registered via `extractor_types`
- `SaudeClient.datasets()` — returns the 19 theme datasets
- `pysus/api/types.py` — added `Saude` origin + 18 new dataset-name
  literals (additive, no behaviour change)

### Source-differentiation policy (per review)

- Same logical dataset on two sources (CNES, PNI/VACINACAO,
  SIM/SINASC/VIGILANCIA, Arboviroses) keeps **separate declarations
  per source** — Saude specs never reference DadosGov/FTP classes
- Linkage across sources happens at merge time via
  `identity.cross_origin_id` (shared CKAN UUID), verified live for
  `arboviroses-dengue` (`4d5e5d44-58a8-4d67-b8aa-4ef1e4b00a1c` on
  both portals)
- Portal overlap verified empirically: 105+/138 SUS slugs present on
  dados.gov.br (same CKAN record, same `metadata_modified`), a few
  SUS-only (e.g. `siasi_banco_obitos`, `tuberculose_sesai`) and a few
  dados.gov-only (e.g. `coronavirus-sus`, `snvesavi`, `srag-2021-e-2022`)

### Verification

- 85 saude tests + 45 metadata tests = 130 passing
- Live smoke: 19 datasets enumerated; ARBOVIROSES content → 4 groups
  → 82/37/34 files with metadata; SISAGUA pattern filtering → 14 groups
- full suite: 956 passed, 1 pre-existing failure (dbf_reader pandas dtype)
- ruff clean

---

## Stage 3 — DEMAS REST query path + File model (complete, 2026-08-19)

### Deliverables

- **`pysus/api/saude/rest.py`** — DEMAS REST helpers:
  - `EndpointSpec` frozen dataclass (path, summary, params, tag, limit)
  - `fetch_swagger()` — fetch and cache the OpenAPI spec with TTL
  - `iter_rows()` — async row-offset paginator (offset=N means skip N
    rows, not a page number); handles both envelope (`{"key": [...]}`)
    and bare-list responses; respects `limit` cap and `page_size`
  - `endpoints_from_swagger()` — extract EndpointSpecs, optionally
    filtered by swagger tag
- **`JSONL` file type** — added to `types.py` (`_validate_file_type`,
  `JSONL` literal) and `extensions.py` (`JSONL` class with `load()`,
  `stream()` chunked, `columns`/`rows` with caching; `_detect_jsonl`
  detector placed before `_detect_json` to avoid misclassification;
  registered in `ExtensionFactory._extensions` as `.jsonl`)
- **`SaudeEndpointFile`** — extends `BaseRemoteFile`: one DEMAS REST
  endpoint persisted as JSONL; `.extension = ".jsonl"`, `.size = 0`,
  `.modify` raises `ValueError`, `.year` parsed from path;
  `_download()` streams all pages into a `.jsonl` file via `iter_rows()`
- **`SaudeEndpointFileExtractor`** — metadata extractor wiring:
  identity.name = path, access.url = full DEMAS URL, format = jsonl,
  download_strategy = http-paged, provenance.origin = saude
- **`SaudeDataset._fetch_content()`** — now returns both `SaudeGroup`
  (CKAN packages) and `SaudeEndpointFile` (DEMAS endpoints) mixed;
  endpoint files sit at the dataset level (no group), matching the
  DuckLake ungrouped-files pattern

### Test coverage

- `test_rest.py` — 20 tests: EndpointSpec, endpoints_from_swagger,
  _extract_rows, iter_rows (basic, empty, limit, params, offset
  advance, bare list), fetch_swagger (cache, no-cache, force-refresh)
- `test_endpoint_file.py` — 17 tests: SaudeEndpointFile properties,
  _download writes JSONL, callback, fetch_size; extractor facets
- `test_extensions.py` — 11 new JSONL tests: columns, rows, empty,
  blank lines, load, stream chunking, column/row caching, detector
  (jsonl, single json, array, empty), factory instantiation
- Full suite: **1006 passed**, 1 pre-existing failure
- ruff clean

---

## Open decisions still pending (roadmap §6)

1. ~~Origin name: `saude`~~ ✅ resolved — added to `types.py` in Stage 2
2. ~~`JSONL` file type literal~~ ✅ resolved — added to `types.py` + `extensions.py` in Stage 3
3. ~~Overlapping datasets home~~ ✅ resolved — source-differentiated
   declarations (Saude registry is source-scoped; linkage via
   `cross_origin_id` at merge time)
4. ~~Sample-driven column metadata default~~ ✅ resolved — deferred;
   JSONL files infer columns from first record at load time
5. ~~Async pagination default for large endpoints~~ ✅ resolved —
   `iter_rows()` uses row-offset pagination with configurable
   `page_size` (default 1000); offset semantics confirmed empirically
