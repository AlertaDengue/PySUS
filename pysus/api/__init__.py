"""PySUS public API for accessing Brazilian public health data.

Provides clients for DuckLake, FTP, and DadosGov data sources,
file format handlers, and high-level convenience functions.
"""

from .client import PySUS as PySUSClient  # noqa
from .errors import (  # noqa
    AuthenticationError,
    CatalogError,
    ConnectionError,
    ConversionError,
    DownloadError,
    FormatError,
    ParseError,
    PySUSError,
    ValidationError,
)
