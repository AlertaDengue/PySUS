# DuckLake Catalog Component

This module provides the application-level models and data adapters required to interface with remote DuckLake resources over S3. It wraps low-level database operations into unified interfaces (`File`, `DuckDataset`, and `DuckGroup`) and implements resilient database connection management via async context managers.

## Features

* **Deterministic Resource Management**: Implements asynchronous context managers (`async with`) across clients and database adapters to prevent DuckDB file locks and connection leaks.
* **Fail-Safe Cleanups**: Features fallback `__del__` destructors to safely terminate remaining active engines during garbage collection or interpreter shutdown.
* **Intelligent Syncing**: Employs an `update_on_close` mechanism to optionally push state changes back to S3 automatically upon exiting a context.
* **SQLAlchemy Eager Loading Fixes**: Optimizes attribute mappings using isolated path strategies (`joinedload` vs. `contains_eager`) based on query parameters.
* **Resilient S3 Verifications**: Gracefully intercepts 404 responses during S3 download handshakes to stop failing connection retry loops early.

---

## Architecture Overview

The system separates the raw database management layer (Adapters) from the client wrapper layer (Client Models). 

1. **Adapters (`BaseAdapter`)**: Track local and remote `.duckdb` target states, manage connections, handle S3 transfers, and expose scoped SQLAlchemy transaction sessions.
2. **Client Components (`DuckLake`)**: Coordinate high-level actions, parse credential models, route queries, and handle collection loops.

---

## Lifecycle & Connection Handling

### Using Context Managers (Recommended)

Using `async with` blocks guarantees deterministic resource teardown. The moment execution exits the context layout block—even due to a runtime crash—all engines are disposed of cleanly.

```python
from pysus.api.ducklake.client import DuckLake

async with DuckLake() as dl:
    datasets = await dl.datasets()

sia = datasets[4] # e.g., SIA
files = await sia.query(state="SP", year=2026)
