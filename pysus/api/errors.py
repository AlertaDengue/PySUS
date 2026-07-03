"""PySUS exception hierarchy."""

from __future__ import annotations


class PySUSError(Exception):
    """Base exception for all PySUS errors."""


class ConnectionError(PySUSError):
    """Failed to connect to a data source (DuckLake, FTP, DadosGov)."""


class AuthenticationError(PySUSError):
    """Authentication or authorization failure (missing/wrong credentials)."""


class DownloadError(PySUSError):
    """File download failure (HTTP, S3, or FTP transfer error)."""


class CatalogError(PySUSError):
    """DuckLake catalog database operation failure (connect, query, schema)."""


class ParseError(PySUSError):
    """Error parsing file contents or file metadata."""


class ConversionError(PySUSError):
    """Error converting between file formats (e.g. DBF -> Parquet)."""


class ValidationError(PySUSError):
    """Input validation error (missing fields, invalid values)."""


class FormatError(PySUSError):
    """File format not recognized or not supported."""
