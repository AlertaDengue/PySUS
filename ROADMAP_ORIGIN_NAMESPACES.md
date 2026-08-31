# Roadmap: origin-namespaced public API (`pysus.ftp.sinan`, `pysus.dadosgov.sinan`, `pysus.saude.*`)

## Goal

Make the data source a first-class, **impossible-to-ignore** part of the public
API. Origin namespaces replace the optional `origin=` kwarg. DuckLake is **not**
an origin — it is the shared catalog/S3 cache that every origin namespace reads
from by default, with the ability to bypass it and hit the origin server
directly:

```python
from pysus import ftp, dadosgov, saude

df = ftp.sinan(disease="deng", year=2020, as_dataframe=True)      # FTP mirror from DuckLake/S3 catalog (default)
df = ftp.sinan(..., source="origin")                               # direct from the DATASUS FTP server
df = dadosgov.sinan(disease="deng", year=2020)                     # DadosGov mirror from catalog (default)
df = dadosgov.sinan(..., source="origin")                          # direct from ckan.saude.gov.br
df = saude.assistenciasaude(..., as_dataframe=True)                # Saude-portal theme dataset

ftp.list_files(dataset="sinan", year=2020)                        # discovery scoped to FTP
dadosgov.info()                                                   # discovery scoped to DadosGov
```

Note: `source` is one of `"catalog"` (default, reads the S3 mirror) or
`"origin"` (queries the origin server directly). The Saude portal has no
catalog mirror, so `saude.*` always queries the CKAN portal regardless of
`source`.

Each namespace is self-contained: it exposes the same class of **per-database
fetchers** AND the same **discovery functions** (`list_files`, `info`) as the
top-level `pysus.__all__`, all scoped to that origin.

This eliminates the bug class behind the DENG/2020 issue: a call that
**silently** served a different snapshot than the caller expected (the default
query returned the DadosGov mirror while the caller assumed FTP).

---

## Existing pieces we already have (leverage, don't rebuild)

- `_fetch_data(dataset, group, state, year, month, origin, ...)` —
  `pysus/api/_impl/databases.py:76` dispatches `origin="SAUDE"` →
  `_fetch_saude`, everything else → `_fetch_ducklake` with `client_filter` from
  `FTP/DUCKLAKE/DADOSGOV` (`databases.py:167-173`).
- Origin aliases + validator — `pysus/api/types.py:128-130`,
  `pysus/api/validate.py`.
- Per-origin catalog metadata already stored (`origin_path`,
  `origin_modified`, `origin_size`, FTP `sha256`).
- The CLI is already origin-namespaced: `pysus/cli/{ftp,dadosgov,saude}.py` —
  the Python API mirrors it.
- The public surface is table-driven: `pysus/api/_impl/__init__.py` `__all__`
  → `pysus/__init__.py`.

---

## Design (decisions locked)

### A. Namespaces

| namespace | default fetch | direct fetch | discovery |
|---|---|---|---|
| `pysus.ftp` | FTP mirror from DuckLake catalog | `source="origin"` | `ftp.list_files`, `ftp.info` |
| `pysus.dadosgov` | DadosGov mirror from DuckLake catalog | `source="origin"` | `dadosgov.list_files`, `dadosgov.info` |
| `pysus.saude` | Saude CKAN/theme (no mirror) | `source="origin"` (no-op) | `saude.list_files`, `saude.info` |

- **No `pysus.ducklake` namespace.** DuckLake is transport/cache, not a source.
- `source="catalog"` is the default in every namespace ("catalog first").
  `source="origin"` downloads directly from the origin server into the local
  cache (eligible for the existing resync/backfill engine). The Saude portal has
  no catalog mirror, so `saude.*` always queries the CKAN portal.
- Namespaced functions reject `origin=` (PySUSError) and invalid `source=`
  (ValidationError).
- **Legacy flat functions** (`pysus.sinan`, ...): deprecated-but-functional —
  keep working, always emit a warning suggesting the namespaced call. Never
  silently change their default behavior.

### B. Function table (single source of truth)

Each origin maps to a set of per-database fetchers **plus** `list_files`/`info`:

| origin | per-database | discovery |
|---|---|---|
| `ftp` | core set applicable on FTP (sinan, sinasc, sim, sih, sia, ciha, cnes, pni, ibge, covid19, ...) | `ftp.list_files`, `ftp.info` |
| `dadosgov` | same core set **filtered to what CKAN actually publishes** | `dadosgov.list_files`, `dadosgov.info` |
| `saude` | the 16 themes (`arboviroses`, `assistenciasaude`, ... `vigilancia_meio_ambiente`) | `saude.list_files`, `saude.info` |

**Phase 0 pre-work**: build + validate the applicability matrix from
`info_table`/`list_files` + catalog so we never expose a `dadosgov.sinasc` that
404s.

**Decision**: DadosGov functions for datasets CKAN does **not** cover are
**omitted** from the namespace — not exposed-and-405. The namespace exposes only
what DadosGov actually publishes.

### C. Namespaced functions via a factory (no copy-paste)

- `_bind_origin(fn, origin)` → wrapper with origin fixed; signature-level param
  validation.
- `build_origin_module(name, origin, fetchers, discovery)` → module with
  `__all__`, docstrings, and `get_origin_meta()` exposing `origin_path`/
  `origin_modified`/`origin_size`/`sha256` (issue Q4).
- Register modules so both `pysus.ftp.sinan(...)` and `from pysus.ftp import
  sinan` work.

---

## Phases

### Phase 0 — Foundation

- [x] Typed internal primitive `fetch(dataset, ..., origin, source)` with
      `origin ∈ {FTP, DadosGov, Saude}`, `source ∈ {catalog, origin}`.
      (Implemented in `pysus/api/_impl/source.py`.)
- [x] Replace the ad-hoc mapping in `_fetch_ducklake` (`databases.py:167-173`)
      with one lookup (`_client_filter` in `source.py`); add the
      direct-origin path (`fetch(source="origin")`).
- [x] Build + lock the origin×dataset applicability matrix
      (`APPLICABILITY` in `source.py`, incl. `list_files`/`info` scoping).
- [x] **Exit**: flat `pysus.sinan` unchanged; suite green (1553 → 1572 tests).
### Phase 1 — Namespace modules

- [x] Factory + `pysus.ftp`, `pysus.dadosgov`, `pysus.saude` modules, each with
      fetchers + discovery. (`_bind_origin`/`build_origin_module`/
      `install_origin_module` in `source.py`; module files at `pysus/{ftp,
      dadosgov,saude}.py`, origin-aware docstrings on module + fetchers +
      discovery.)
- [x] Register on package (`pysus/__init__.py`); verify both access styles and
      `from pysus.ftp import sinan`.
- [x] Catalog-first default returns exactly today's `origin="FTP"`/
      `origin="DadosGov"` results (verified `pysus.ftp.sinan(DENG,2017)`
      = 239,395 rows, matching `origin="FTP"`).
- [x] **Exit**: `pysus.ftp.sinan(...)` ≡ `pysus.sinan(..., origin="FTP")`;
      `source="origin"` returns the same DataFrame after a live pull.

### Phase 2 — Forced-verbose semantics

- [x] Reject `origin=` and invalid `source=` in namespaced calls.
      (`_bind_origin` raises `PySUSError` on a stray `origin=`; invalid
      `source=` raises `ValidationError`.)
- [x] Legacy flat functions: deprecation warning + docstring pointing to the
      namespaces; behavior unchanged. (`_deprecate_flat` wraps every flat
      fetcher/`list_files` in `databases.py`; namespaced wrappers suppress via
      `_suppress_flat_deprecation`, so only a *direct* flat call is flagged.)
- [x] Document `get_origin_meta()` per namespace (docstring on the bound
      `get_origin_meta`).
- **Exit**: pre-commit chain green (1589 tests); flat call warns once per
      call, namespaced call is silent.

### Phase 3 — Tests & CI

- [ ] `pysus/tests/api/test_origins.py`:
  - namespaces exist with correct `__all__` (fetchers + discovery),
    bound-wrapper identity;
  - `pysus.ftp.sinan` routes to `client_filter=FTP`; `source="origin"` routes to
    live FTP path;
  - invalid `origin=` / `source=` rejected; discovery scoped per origin;
  - `from pysus.ftp import sinan`, `pysus.ftp.list_files`,
    `pysus.saude.assistenciasaude`; idempotent imports.
- [ ] `test_databases.py`: flat calls now warn (but still return identical data).
- **Exit**: full suite green (256 existing + new).

### Phase 4 — Docs & UX

- [ ] Docstrings + README lead with namespaced examples.
- [ ] `info_table`/`search` show per-origin hints (`pysus.ftp.sinan`).
- [ ] Migration guide (flat → namespaced; warning text), changelog, version bump.
- **Exit**: docs render; `pysus.info()` shows the namespaced call per row.

### Phase 5 — Rollout & issue closure

- [ ] Merge, tag, publish.
- [ ] Post follow-up on the DENG/2020 issue with the new syntax + comparison
      table.

---

## Risks & mitigations

- **Default `source` staleness**: catalog-first can serve a stale mirror →
  mitigated by `get_origin_meta()` (user sees `origin_modified`);
  `stale_check`/`max_age` can be layered on later without changing the surface.
- **Import-time module generation / circular imports** → factory resolves
  `_fetch_data` at call time.
- **`help()`/signature introspection** → generated wrappers get explicit
  `__name__`/`__doc__`/signatures.
- **DadosGov gaps** → functions CKAN doesn't cover are **omitted** from the
  namespace (decision above).
- **Name shadowing**: `pysus.ftp` (API) vs `pysus.cli.ftp` are distinct
  namespaces — no conflict.
