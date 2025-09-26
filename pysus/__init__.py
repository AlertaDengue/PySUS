# type: ignore[attr-defined]
"""PySUS Python package"""

from importlib import metadata as importlib_metadata

from pysus.ftp.databases import *  # noqa


def get_version() -> str:
    try:
        return importlib_metadata.version(__name__)
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return "1.0.0"  # changed by semantic-release


version: str = get_version()
__version__: str = version
