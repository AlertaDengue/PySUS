"""PySUS management: cross-client file tracking, comparison and sync.

The management package implements the workflow that keeps the S3 bucket
and its DuckLake catalogs in sync with the FTP and DadosGov clients:

* :mod:`records` — origin-agnostic file records, identity keys and
  sync reports;
* :mod:`inventory` — collect listings from every client + snapshots;
* :mod:`compare` — cross-client identity grouping and content
  fingerprints;
* :mod:`catalog` — parameterized metadata upserts into DuckLake;
* :mod:`sync` — the end-to-end pipeline (inventory → compare →
  download → parquet → upload → catalog);
* :mod:`normalize` — S3 key canonicalization utilities;
* :mod:`client` — ``CatalogManager`` facade for single-file uploads.
"""

from .catalog import CatalogWriter, sha256_of  # noqa
from .client import CatalogManager  # noqa
from .compare import Comparator, content_fingerprint  # noqa
from .inventory import Inventory  # noqa
from .normalize import BucketNormalizer  # noqa
from .records import (  # noqa
    DOWNLOAD_PRIORITY,
    KEY_MISSING,
    NATIONAL_STATE,
    FileComparison,
    FileRecord,
    IdentityKey,
    SnapshotDiff,
    SyncOutcome,
    SyncReport,
    base_stem,
    canonical_dataset,
    canonical_group,
    compose_s3_key,
    format_of,
    parquet_key,
    stem_of,
)
from .sync import SyncEngine  # noqa

__all__ = [
    "BucketNormalizer",
    "CatalogManager",
    "CatalogWriter",
    "Comparator",
    "DOWNLOAD_PRIORITY",
    "FileComparison",
    "FileRecord",
    "IdentityKey",
    "Inventory",
    "KEY_MISSING",
    "NATIONAL_STATE",
    "SnapshotDiff",
    "SyncEngine",
    "SyncOutcome",
    "SyncReport",
    "base_stem",
    "canonical_dataset",
    "canonical_group",
    "compose_s3_key",
    "content_fingerprint",
    "format_of",
    "parquet_key",
    "sha256_of",
    "stem_of",
]
