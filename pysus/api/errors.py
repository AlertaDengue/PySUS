"""PySUS exception hierarchy.

Every error carries a hint (how to fix it) and a docs_url
(link to relevant documentation).  The str method renders
a user-friendly box with the error name, message, hint, and docs link.
"""

from __future__ import annotations

import warnings


class PySUSError(Exception):
    """Base exception for all PySUS errors."""

    hint: str = ""
    docs_url: str = "https://pysus.readthedocs.io/en/latest/errors"

    def __init__(
        self, message: str = "", *, hint: str = "", docs_url: str = ""
    ) -> None:
        super().__init__(message)
        if hint:
            self.hint = hint
        if docs_url:
            self.docs_url = docs_url

    def __str__(self) -> str:
        msg = super().__str__()
        lines = [
            f"  PySUS {type(self).__name__}",
            "",
            f"  {msg}" if msg else "",
        ]
        if self.hint:
            lines.append(f"  Hint: {self.hint}")
        if self.docs_url:
            lines.append(f"  Docs: {self.docs_url}")

        inner = "\n".join(ln for ln in lines if ln is not None)
        width = max(len(ln) for ln in inner.split("\n")) if inner else 40
        width = max(width, 40)

        box = [
            f"╔{'═' * (width + 2)}╗",
            f"║  {type(self).__name__:<{width}}║",
            f"╠{'═' * (width + 2)}╣",
        ]
        for line in inner.split("\n"):
            box.append(f"║  {line:<{width}}║")
        box.append(f"╚{'═' * (width + 2)}╝")

        return "\n".join(box)


class ConnectionError(PySUSError):
    """Failed to connect to a data source (DuckLake, FTP, DadosGov)."""

    hint = (
        "Check your network connection and verify the data source is available."
    )


class AuthenticationError(PySUSError):
    """Authentication or authorization failure (missing/wrong credentials)."""

    hint = "Set DADOSGOV_TOKEN env var or pass token= to the client."


class DownloadError(PySUSError):
    """File download failure (HTTP, S3, or FTP transfer error)."""

    hint = "Check your network connection and verify the file exists."


class CatalogError(PySUSError):
    """DuckLake catalog database operation failure (connect, query, schema)."""

    hint = "Ensure the DuckLake catalog is initialized (pysus ducklake init)."


class ParseError(PySUSError):
    """Error parsing file contents or file metadata."""

    hint = "The file may be corrupted. Try re-downloading."


class ConversionError(PySUSError):
    """Error converting between file formats (e.g. DBF -> Parquet)."""

    hint = "The source file may be corrupted or in an unsupported format."


class ValidationError(PySUSError):
    """Input validation error (missing fields, invalid values)."""

    hint = "Check the function signature and required parameters."


class FormatError(PySUSError):
    """File format not recognized or not supported."""

    hint = "Ensure the file is a supported format (Parquet, DBF, CSV, etc.)."


class PySUSWarning(UserWarning):
    """Non-fatal warning for degraded operation."""

    pass


def warn(message: str, *, stacklevel: int = 2) -> None:
    """Emit a PySUSWarning."""
    warnings.warn(message, PySUSWarning, stacklevel=stacklevel)
