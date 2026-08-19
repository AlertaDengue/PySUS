# Stage 1 Spike: Saude catalog + downloads client

**PR scope:** port `epidatasets.sources.opendatasus.OpenDataSUSAccessor`
into PySUS as a new, isolated `pysus/api/saude/` package. **No** base-class
changes, **no** new origin enum value, **no** new `FileType`. Ships value
on its own (browse + download the SUS open-data catalog) and unblocks
Stages 2–6.

Estimated effort: **2 days**.

---

## 1. Goal

A user can:

```python
from pysus.api.saude import SaudeClient
async with SaudeClient() as c:
    datasets = await c.list_datasets(group="arboviroses")
    meta = await c.fetch_dataset("arboviroses-dengue")
    paths = await c.download_dataset("arboviroses-dengue", fmt="CSV")
```

and a CLI user can:

```bash
pysus-saude list-datasets --group arboviroses
pysus-saude show arboviroses-dengue
pysus-saude download arboviroses-dengue --fmt CSV --dest ./out/
```

## 2. Non-goals (explicitly deferred)

- ❌ Adding `"saude"` to `Origin` / new `DatasetName` literals in
  `pysus/api/types.py` — done in Stage 2.
- ❌ The `MetadataBag` refactor in §1 of `roadmap_saude.md` — done in
  Stage 0 (a separate PR).
- ❌ Touching `BaseRemoteFile`, `BaseRemoteClient`, etc. — Stage 0 again.
- ❌ SyncEngine / DuckLake / S3 catalog integration — Stages 4–6.
- ❌ The DEMAS REST query path (the 87 paginated JSON endpoints) —
  Stages 2–3. This spike is **catalog only**.
- ❌ Reusing `ExtensionFactory` / `to_parquet` — Stages 5+ (just write
  the raw CSV.ZIP to disk in this spike).
- ❌ Pandas anywhere in `pysus/api/saude/` — the reference's DataFrame
  helpers are not ported (per §0.5.3).

## 3. File tree (new files only — no edits to existing files)

```
pysus/api/saude/
    __init__.py            # exports SaudeClient
    client.py              # async SaudeClient (httpx-based)
    next_data.py           # buildId extraction (regex + cache)
    catalog.py             # Next.js data fetcher (catalog/dataset)
    resources.py           # resource models (CKAN dict → pydantic)
    download.py            # resource / dataset download helpers
    errors.py              # SaudeError, BuildIdMissing, PortalChanged

pysus/tests/api/saude/
    __init__.py
    conftest.py            # offline httpx MockTransport fixtures
    fixtures/
        homepage.html                  # captured 2026-08
        catalog_page1.json             # captured 2026-08
        dataset_arboviroses-dengue.json # captured 2026-08
        resource_csv_dengue_2024.csv.zip # tiny sample (hand-crafted)
    test_next_data.py
    test_catalog.py
    test_resources.py
    test_download.py
    test_client.py

docs/source/guides/saude.rst
CHANGELOG.md              # one new entry under Unreleased
```

## 4. Per-file plan

### 4.1 `pysus/api/saude/next_data.py`

Port `_NEXT_DATA_RE` and the buildId extraction logic from
`epidatasets.sources.opendatasus.OpenDataSUSAccessor._get_build_id`.

Differences from the reference:

- **Async** — uses `httpx.AsyncClient`, not `requests`.
- **No in-memory `self._build_id`** — relies on the disk cache only.
  The async context manager handles short-lived lifetimes.
- **Typed errors** — raises `SaudeError` with a `.kind` enum
  (`BuildIdMissing`, `PortalChanged`) instead of generic `RuntimeError`.
- **Cache path** — `CACHEPATH / "saude" / "build_id.json"` (matches the
  existing `CACHEPATH` constant in `pysus/__init__.py`).

```python
_NEXT_DATA_RE = re.compile(
    r'__NEXT_DATA__"\s+type="application/json">(.*?)</script>',
    re.DOTALL,
)

async def fetch_build_id(
    client: httpx.AsyncClient,
    cache_path: Path,
    ttl: timedelta = timedelta(hours=24),
) -> str: ...
```

### 4.2 `pysus/api/saude/catalog.py`

Port `_search_params()`, `_fetch_catalog_page()`, `list_datasets()`,
`list_datasets_all()`, `list_groups()`, `list_tags()`, `get_dataset()`.
Drop `_packages_to_frame()`, `get_dataset_metadata()` (DataFrame-shaped),
`list_countries()` (PySUS has `pysus.api.types.State` for that).

Differences from the reference:

- **Async** — every function is `async def`, uses `httpx.AsyncClient`.
- **Returns dataclasses / pydantic models**, not DataFrames.
- **Pagination is an `async iterator`** so callers can break early:

  ```python
  async def iter_datasets(
      *, group: str | None = None, tag: str | None = None,
      fmt: str | None = None, q: str | None = None,
  ) -> AsyncIterator[CatalogEntry]: ...
  ```

  `list_datasets()` and `list_datasets_all()` are thin wrappers that
  materialize the iterator.
- **Per-URL TTL cache** — `_get_json()` is ported as `fetch_json()`
  (no DataFrame-specific branches needed).
- **Retry policy** — exponential backoff, max 3 attempts, on
  `httpx.HTTPError` and `httpx.TransportError`. Match the existing
  pattern in `pysus/management/sync.py:_RETRYABLE`.
- **Cache key** — explicit `cache_key: str | None = None` parameter
  (mirrors reference). The cache lives at
  `CACHEPATH / "saude" / "catalog" / <cache_key>.json`.

Models:

```python
@dataclass(slots=True)
class CatalogEntry:
    name: str                   # slug, e.g. "arboviroses-dengue"
    title: str
    notes: str
    formats: list[str]          # ["PDF", "CSV", "JSON", "XML"]
    groups: list[GroupRef]
    tags: list[TagRef]

@dataclass(slots=True)
class GroupRef:
    name: str                   # "arboviroses"
    display_name: str           # "Arboviroses"

@dataclass(slots=True)
class TagRef:
    name: str
    display_name: str
```

### 4.3 `pysus/api/saude/resources.py`

**Pydantic models** for the CKAN package and resource dicts. This is
the *one* file that diverges materially from the reference (the
reference returns raw dicts).

```python
class CKANPackage(BaseModel):
    """The full package dict returned by /dataset/<slug>.json.

    Fields documented in roadmap_saude.md §0.5.2 — 30 keys.
    Extra keys are accepted and ignored (CKAN adds new fields over time).
    """
    model_config = ConfigDict(extra="ignore")

    id: str                              # UUID
    name: str                            # slug
    title: str
    notes: str = ""
    author: str | None = None
    author_email: str | None = None
    creator_user_id: str | None = None
    isopen: bool = True
    license_id: str | None = None
    license_title: str | None = None
    license_url: str | None = None
    maintainer: str | None = None
    maintainer_email: str | None = None
    metadata_created: datetime
    metadata_modified: datetime
    num_resources: int
    num_tags: int = 0
    organization: Organization | None = None
    owner_org: str | None = None
    private: bool = False
    state: str = "active"
    type: str = "dataset"
    url: str | None = None
    version: str | None = None
    extras: list[Extra] = Field(default_factory=list)
    groups: list[GroupRef] = Field(default_factory=list)
    tags: list[TagRef] = Field(default_factory=list)
    resources: list[Resource] = Field(default_factory=list)
    # relationships_* omitted — rarely used, adds noise

    @property
    def periodicity(self) -> str | None: ...   # from extras[]
    @property
    def contact(self) -> str | None: ...       # from extras[]

class Resource(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str
    name: str
    description: str = ""
    format: str = ""                          # "CSV", "JSON", "XML", "PDF", "API"
    url: str
    size: int | None = None
    mimetype: str | None = None
    created: datetime | None = None
    last_modified: datetime | None = None
    metadata_modified: datetime | None = None
    position: int = 0
    hash: str = ""

class Organization(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str
    name: str
    title: str | None = None
    display_name: str | None = None
    description: str | None = None
    created: datetime | None = None
    approval_status: str | None = None
    state: str | None = None

class Extra(BaseModel):
    model_config = ConfigDict(extra="ignore")
    key: str
    value: str
```

Validator notes:

- `metadata_created` / `metadata_modified` use `BeforeValidator` with
  `fromisoformat` (CKAN emits `2024-02-22T19:23:06.206939` — Python 3.11+
  handles this natively).
- `created` / `last_modified` on `Resource` are nullable.
- `format` is normalised to uppercase before validation.

### 4.4 `pysus/api/saude/download.py`

Port `download_resource()` and `download_dataset()` from the reference.
Port `_filename_for()` verbatim.

Differences from the reference:

- **Async** — uses `httpx.AsyncClient.stream()` with
  `await client.arequest()` patterns matching
  `pysus/api/dadosgov/client.py:269-297`.
- **No DataFrame return** — both helpers return `list[Path]`.
- **Progress callback** — mirrors `pysus.api.dadosgov.client.download`:
  ```python
  async def download_resource(
      package: CKANPackage,
      resource_id: str | None = None,
      name: str | None = None,
      fmt: str | None = None,
      dest_dir: Path | None = None,
      *,
      progress: Callable[[int, int], None] | None = None,
  ) -> Path: ...
  ```
- **API-format skip** — keep the reference's `format != "API"` filter
  (API resources are documentation links, not files).

### 4.5 `pysus/api/saude/client.py`

Thin facade — owns the `httpx.AsyncClient`, exposes the high-level
methods:

```python
class SaudeClient:
    def __init__(
        self,
        *,
        cache_dir: Path | None = None,
        cache_ttl: timedelta = timedelta(hours=24),
        timeout: float = 30.0,
    ): ...

    async def __aenter__(self) -> "SaudeClient": ...
    async def __aexit__(self, *exc) -> None: ...

    # Catalog
    async def list_datasets(
        self, *, q: str | None = None, group: str | None = None,
        tag: str | None = None, fmt: str | None = None,
        page: int = 1,
    ) -> list[CatalogEntry]: ...

    async def iter_datasets(
        self, *, q: str | None = None, group: str | None = None,
        tag: str | None = None, fmt: str | None = None,
        max_pages: int | None = None,
    ) -> AsyncIterator[CatalogEntry]: ...

    async def list_groups(self) -> list[GroupRef]: ...
    async def list_tags(self) -> list[TagRef]: ...

    async def fetch_dataset(self, slug: str) -> CKANPackage: ...
    async def fetch_resources(self, slug: str) -> list[Resource]: ...

    # Downloads
    async def download_resource(
        self, slug: str, *, resource_id: str | None = None,
        name: str | None = None, fmt: str | None = None,
        dest_dir: Path | None = None,
        progress: Callable[[int, int], None] | None = None,
    ) -> Path: ...

    async def download_dataset(
        self, slug: str, *, dest_dir: Path | None = None,
        fmt: str | None = None,
        progress: Callable[[int, int], None] | None = None,
    ) -> list[Path]: ...
```

### 4.6 `pysus/api/saude/__init__.py`

One line: `from .client import SaudeClient  # noqa`.

### 4.7 `pysus/api/saude/errors.py`

```python
class SaudeError(Exception):
    """Base error for the Saude client."""

class BuildIdMissing(SaudeError):
    """__NEXT_DATA__ block not found on the homepage."""

class PortalChanged(SaudeError):
    """Catalog response schema differs from the expected one."""

class DatasetNotFound(SaudeError):
    """Requested slug does not exist on the portal."""

class ResourceNotFound(SaudeError):
    """No matching downloadable resource for the given selector."""
```

### 4.8 CLI: `pysus/cli/saude.py` + `pysus/cli/__init__.py`

Mirrors the existing CLI structure (see
`pysus/management/scripts/sync_clients.py:33-60` for the pattern).
Subcommands:

```
pysus-saude list-datasets [--group X] [--tag Y] [--fmt Z] [--q TEXT]
pysus-saude list-groups
pysus-saude show SLUG
pysus-saude download SLUG [--fmt CSV] [--dest DIR]
```

Uses `argparse` (no third-party CLI lib — matches the rest of PySUS).

### 4.9 Documentation

`docs/source/guides/saude.rst` — mirror of
`docs/source/guides/dadosgov.rst`. One section per public method, plus
a "Verifying offline" section that points at the fixtures so users can
test the client without hitting the portal.

Add to `docs/source/guides/index.rst`:
```rst
   saude
```

Add to `docs/source/index.rst` (the API overview, if it lists guides):
the same line.

## 5. Test plan

### 5.1 Fixtures (committed under `pysus/tests/api/saude/fixtures/`)

These are **real captured payloads** (one per portal surface):

| Fixture | Source | Size |
|---|---|---|
| `homepage.html` | `curl https://dadosabertos.saude.gov.br/` | ~33 KB |
| `catalog_page1.json` | captured from `/_next/data/<id>/dataset.json?page=1` | ~15 KB |
| `dataset_arboviroses-dengue.json` | captured from `/_next/data/<id>/dataset/arboviroses-dengue.json` | ~30 KB |
| `resource_csv_dengue_2024.csv.zip` | captured from the S3 URL in `arboviroses-dengue` resources (or a 1-row synthetic zip) | ~1 KB synthetic |

The captured payloads are regenerated by the spike author before merge;
they live in-repo so tests run offline.

### 5.2 `conftest.py` — `httpx.MockTransport` fixtures

```python
@pytest.fixture
def mocked_saude(fixtures_dir: Path) -> httpx.MockTransport:
    """Mock httpx transport that serves the captured fixtures."""
    routes = {
        "dadosabertos.saude.gov.br/": "homepage.html",
        "/_next/data/": "catalog_page1.json",
        "/dataset/arboviroses-dengue.json": "dataset_arboviroses-dengue.json",
        ...
    }
    return httpx.MockTransport(_handler(routes, fixtures_dir))

@pytest.fixture
def client(mocked_saude) -> SaudeClient:
    return SaudeClient(cache_dir=tmp_path / "cache")
```

### 5.3 Test modules

| Module | What it covers |
|---|---|
| `test_next_data.py` | `fetch_build_id()` extracts the right value, caches to disk, falls back to disk when homepage is unreachable, raises `BuildIdMissing` if no `<script>` block |
| `test_catalog.py` | `iter_datasets()` yields all 20/page; `list_groups()` returns the 14; `fetch_dataset("arboviroses-dengue")` returns a fully-populated `CKANPackage`; cache hit on second call; `PortalChanged` when the response shape differs |
| `test_resources.py` | All 19 `Resource` fields parse; `periodicity` and `contact` properties pull from `extras[]`; `format` is uppercased |
| `test_download.py` | `download_dataset("arboviroses-dengue", fmt="CSV")` writes a `.csv` file to disk; `API` resources are skipped; `_filename_for()` produces safe names |
| `test_client.py` | Async context manager works; one integration test that drives the full flow with mocks |

### 5.4 Acceptance criteria (CI must pass)

- `pytest pysus/tests/api/saude/ -q` is green offline (no network).
- `pytest pysus/tests/api/saude/ -q --network` is green against the live
  portal (one smoke test, marked `network`).
- `ruff check pysus/api/saude/` clean.
- `pyright pysus/api/saude/` clean (matches the existing project
  config in `pyrightconfig.json`).
- `pysus-saude --help` lists all 4 subcommands.

## 6. Risks specific to this spike

1. **buildId rotation mid-test** — mitigated by the disk cache; the
   test fixtures pin a specific buildId.
2. **Portal redesign** — captured in the `PortalChanged` error path;
   tests cover the regression.
3. **Large `dataset.json` payload** (~30 KB for arboviroses-dengue;
   probably more for richer packages) — no streaming; the full payload
   is parsed in memory. Fine for the spike; revisit if a package
   exceeds 1 MB.
4. **httpx vs requests behavioural differences** — `requests` is sync;
   `httpx.AsyncClient.stream()` semantics differ slightly. The tests
   cover the streaming download path with a mock transport that
   chunks.

## 7. Out-of-scope follow-ups (filed as TODOs in code)

Each method gets a `# TODO(stage-N): ...` comment so the next
maintainer can find them:

```python
# TODO(stage-0): wrap in MetadataBag, populate identity.cross_origin_id
# TODO(stage-2): wire as SaudeDatasetExtractor
# TODO(stage-3): add DEMAS-REST as a complementary catalog source
```

## 8. Rollback plan

This spike touches **zero existing files** (only new files under
`pysus/api/saude/`, `pysus/tests/api/saude/`, `pysus/cli/saude.py`,
`docs/source/guides/saude.rst`, and a single CHANGELOG entry). A bad
merge is a one-line revert.

The CLI subcommand is opt-in (`pysus-saude`), so users who don't
install it (or pin the version) are unaffected.

---

## 9. Quick reference: what to read first

1. The `epidatasets` reference:
   `epidatasets/sources/opendatasus.py` (~300 lines, the entire source).
2. `pysus/api/dadosgov/client.py:269-297` — async download pattern.
3. `pysus/management/sync.py:_RETRYABLE` — retry/backoff idiom.
4. `pysus/api/extensions.py:1140-1232` — `ExtensionFactory` (NOT used
   in this spike, but the pattern to mirror in Stage 5).
5. `pysus/management/scripts/sync_clients.py:33-60` — CLI script pattern.
