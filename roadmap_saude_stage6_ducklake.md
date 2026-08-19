# Stage 6 — DuckLake catalog integration

## Analysis

Most of Stage 6 is **already working** through the existing pipeline:

- **Task 1 (per-dataset catalogs):** `CatalogWriter.ensure_dataset()` + `DatasetAdapter` already auto-create `catalog_{name}.duckdb` for any dataset name. No code change needed.
- **Task 2 (register dataset names):** Done in Stage 2 — `types.py` has all 18 Saude dataset literals.
- **Task 3 (column metadata):** Auto-inferred from Parquet conversion via `link_columns()`. The roadmap suggests manual YAML overrides as future enhancement, not required for MVP.
- **Task 4 (DuckLake.datasets()):** Returns ALL datasets from central catalog. No code change needed.
- **Task 6 (Frontend):** Deferred — not required for integration.

## What needs to be done

### 6.A — Fix `DuckDataset.query()` state=NULL handling

**File:** `pysus/api/ducklake/models.py:183-186`

Current code:
```python
if states:
    stmt = stmt.filter(CatalogFile.state.in_([s.upper() for s in states]))
```

**Problem:** SQL `IN` never matches `NULL`. Saude files are national-only (no state), so `state IS NULL`. When querying with `state=["SP"]`, these files are excluded.

**Fix:** Add `or_(CatalogFile.state.in_(states), CatalogFile.state.is_(None))`.

### 6.B — Integration test: Saude dataset visible in DuckLake

Write a test that:
1. Creates a DuckLake instance with in-memory catalogs
2. Inserts a Saude dataset row via `CatalogWriter.ensure_dataset()`
3. Inserts file rows via `CatalogWriter.upsert_file()`
4. Asserts `DuckLake.datasets()` returns the Saude dataset
5. Asserts `DuckDataset.query()` returns the files

### 6.C — Verify sync pipeline writes correct catalog entries

Verify `_catalog_rows()` in `sync.py` handles the `"saude"` origin correctly — origin is passed through to `upsert_file()`.

### 6.D — Progress MD + commit

## Not needed (already working)

- Per-dataset catalog creation (automatic via DatasetAdapter)
- Dataset name registration (done in Stage 2)
- Column metadata (auto-inferred from Parquet)
- Central catalog listing (returns all datasets)
