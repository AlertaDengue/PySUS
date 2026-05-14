"""DuckLake subpackage for interacting with the PySUS S3 catalog.

Provides a DuckDB-based client for querying and downloading
public health datasets stored in object storage.
"""

from .client import DuckLake as DuckLakeClient  # noqa
