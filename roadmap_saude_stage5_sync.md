# Stage 5 — Sync engine integration

**Goal:** Saude endpoints become first-class sync targets, with
idempotent, resumable ingestion through the existing
inventory → compare → sync pipeline.

## Status

⏳ not started

## Scope analysis

The sync engine (`pysus/management/sync.py`, 1279 lines) is
origin-agnostic at the data model level (all files become `FileRecord`)
but has several origin-specific branches. Stage 5 adds `"saude"` to
these branches and ensures the JSONL → parquet conversion chain works.

### What already works

- `FileRecord` with `origin="saude"` — delivered in Stage 4
- `DOWNLOAD_PRIORITY` includes `"saude"` as 4th fallback — Stage 4
- `Inventory._collect_saude()` produces `FileRecord` objects — Stage 4
- `JSONL.to_parquet()` — inherited from `BaseTabularFile`, verified working
- `stem_of()` cross-origin identity — no override needed (Stage 4)
- `ExtensionFactory` recognises `.jsonl` — Stage 3

### What needs changing

#### 5.1 — upload_file type annotation

**File:** `pysus/management/sync.py:212-218`

`upload_file()` signature is typed `file: FTPFile | APIFile`. This
excludes `SaudeFile` and `SaudeEndpointFile`. Two options:

- **A (preferred):** Change to `file: BaseRemoteFile` — all our file
  types extend it, and the method only calls `file._download()`,
  `file.path`, `file.size`, `file.modify`, `file.client.name`.
- **B:** Add `| SaudeFile | SaudeEndpointFile` union — verbose, fragile.

Recommendation: **Option A**. The method body already only uses the
`BaseRemoteFile` interface.

#### 5.2 — _download_raw_with_retry type annotation

**File:** `pysus/management/sync.py:384-389`

Same issue: `file: FTPFile | APIFile`. Change to `file: BaseRemoteFile`.

#### 5.3 — _preconnect_adapters origin filter

**File:** `pysus/management/sync.py:946`

Currently:
```python
if r.origin in ("ftp", "dadosgov")
```

Change to:
```python
if r.origin in ("ftp", "dadosgov", "saude")
```

#### 5.4 — _pick_source fallback

**File:** `pysus/management/sync.py:1148-1156`

Currently tries FTP → DadosGov. Add Saude as 3rd fallback:

```python
@staticmethod
def _pick_source(comparison: FileComparison) -> FileRecord | None:
    record = comparison._pick("ftp")
    if record and record.file is not None:
        return record
    record = comparison._pick("dadosgov")
    if record and record.file is not None:
        return record
    record = comparison._pick("saude")
    if record and record.file is not None:
        return record
    return None
```

#### 5.5 — run() inventory collection

**File:** `pysus/management/sync.py:549-556`

Currently collects `ducklake`, `ftp`, and optionally `dadosgov`. Add
`saude` unconditionally (no auth needed):

```python
records["saude"] = await inventory.collect("saude", datasets=dataset_filter)
```

Insert after the dadosgov collection block.

#### 5.6 — run() partitioning

**File:** `pysus/management/sync.py:600-601`

Currently partitions into `ftp_items` (origin==ftp) and `gov_items`
(origin!=ftp). The `gov_items` branch already covers non-ftp origins,
so Saude items will land there naturally. **No change needed** — verify
this works.

#### 5.7 — _reprocess source iteration

**File:** `pysus/management/sync.py:1221-1279`

`_reprocess()` iterates `DOWNLOAD_PRIORITY[1:]` (skipping ducklake),
special-casing DadosGov for token checks. Add Saude handling:

- Saude needs no token — skip the `needs_token` check for
  `origin == "saude"`.
- The loop body calls `upload_file(file.record.file)` — this will
  work once the type annotation is widened (5.1).

#### 5.8 — _download_once fallback chain

**File:** `pysus/management/sync.py:420-471`

`_download_once()` tries: pooled FTP client → `file.client.download()`
→ `file._download()`. For Saude files, `file._download()` handles
everything (CKAN resources or DEMAS paged download). The fallback
chain already reaches `_download()` as a last resort. **Verify no
special handling needed** — `SaudeFile._download()` and
`SaudeEndpointFile._download()` already work.

#### 5.9 — upload_file conversion pipeline

**File:** `pysus/management/sync.py:299-328`

The conversion call at line ~305:
```python
local_file = await ExtensionFactory.instantiate(raw_path)
parquet_file = await local_file.to_parquet(...)
```

This already works for JSONL (verified). The `ExtensionFactory`
detects `.jsonl` and instantiates `JSONL`, which inherits
`to_parquet()` from `BaseTabularFile`. **No change needed.**

#### 5.10 — _fix_misparsed_metadata

**File:** `pysus/management/sync.py:1022-1146`

Hardcodes `origin="ftp"` when recomputing S3 keys. Saude records
won't enter this code path (it only fires for records that already
exist in the catalog with bad month values). **No change needed.**

#### 5.11 — _dedupe_s3_artifacts

**File:** `pysus/management/sync.py:951-1020`

Operates on catalog records, not origin-specific. **No change needed.**

#### 5.12 — CLI --saude-only flag

**File:** `pysus/management/scripts/sync_clients.py:33-60`

Add `--saude-only` flag. When set, restrict `dataset_filter` to only
Saude datasets and skip FTP/DadosGov/DuckLake collection. This is
useful for initial ingestion of Saude-only datasets.

Implementation: add argparse flag, pass through to `run()`.

#### 5.13 — download_to_parquet path

**File:** `pysus/api/client.py:289-301`

The `client_name` switch needs `elif client_name == "saude":`.
Already done in Stage 4 (get_saude() added). Verify the
`download_to_parquet()` method dispatches correctly.

#### 5.14 — JSONL.to_parquet() override (optional)

**File:** `pysus/api/extensions.py`

The inherited `BaseTabularFile.to_parquet()` works but streams the
entire file into memory via `pd.DataFrame`. For very large DEMAS
endpoints (millions of rows), a chunked JSONL → parquet writer would
be more memory-efficient. **Defer to Stage 7** unless profiling shows
issues.

### What does NOT need changing

- `records.py:compose_s3_key()` — already accepts any `origin` string
- `records.py:stem_of()` / `base_stem()` — already handles `.jsonl`
- `catalog.py:CatalogWriter` — schema-driven, no origin branching
- `catalog.py:ensure_dataset()` — accepts any dataset name
- `catalog.py:upsert_file()` — uses `origin` and `format` as free strings
- `compare.py:Comparator` — already groups by `IdentityKey`, origin-agnostic
- `compare.py:_dedup_origin_formats()` — `FORMAT_PREFERENCE` fallback for
  unknown formats already works for `"jsonl"` (rank = len(FORMAT_PREFERENCE))
- `JSONL` class — `load()`, `stream()`, `columns`, `rows` all working

## Execution plan

Ordered by dependency. Each subtask should be a single commit.

### 5.A — Type annotations (low risk, unblocks everything)

Files: `pysus/management/sync.py`
Changes: Widen `upload_file()` and `_download_raw_with_retry()` signatures
from `FTPFile | APIFile` to `BaseRemoteFile`.
Tests: existing sync tests still pass (no behaviour change).

### 5.B — Origin wiring (medium risk, core integration)

Files: `pysus/management/sync.py`
Changes:
- `_preconnect_adapters`: add `"saude"` to origin filter
- `_pick_source`: add Saude as 3rd fallback
- `run()`: add Saude inventory collection
- `_reprocess`: handle Saude in the source iteration loop
Tests: new test cases in `test_sync.py` for each change.

### 5.C — CLI flag (low risk)

Files: `pysus/management/scripts/sync_clients.py`
Changes: add `--saude-only` argparse flag, wire through `run()`.
Tests: CLI smoke test (if feasible) or manual verification.

### 5.D — download_to_parquet dispatch (already done)

Files: `pysus/api/client.py`
Changes: verify the `client_name` switch at line 289-301 handles
`"saude"`. Already done in Stage 4 — confirm with a test.

### 5.E — Integration tests

Files: `pysus/tests/management/test_sync.py`
Changes: new parametrized test cases:
1. Saude-only file (no FTP/DadosGov equivalent) → ingested via
   JSONL → parquet pipeline
2. Saude file with existing FTP copy (FTP wins priority) → skipped
3. Saude endpoint file download → JSONL → parquet round-trip

### 5.F — Progress MD + commit

Update `roadmap_saude_progress.md` Stage 5 status.

## Verification checklist

- [ ] `python -m pysus.management.scripts.sync_clients --datasets ARBOVIROSES`
      ingests endpoint files from Saude when no FTP/DadosGov copy exists
- [ ] `upload_file()` accepts `SaudeFile` and `SaudeEndpointFile` without
      type errors
- [ ] `_pick_source()` falls through to Saude when FTP and DadosGov
      records are absent
- [ ] `_preconnect_adapters()` pre-connects adapters for Saude datasets
- [ ] JSONL → parquet conversion produces valid parquet with correct schema
- [ ] Existing sync tests pass unchanged
- [ ] ruff clean
