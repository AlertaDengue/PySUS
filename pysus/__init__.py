"""PySUS Python package"""

import os
import pathlib
from typing import Final
from importlib import metadata as importlib_metadata


CACHEPATH: Final[str] = os.getenv(
    "PYSUS_CACHEPATH",
    os.path.join(str(pathlib.Path.home()), "pysus"),
)

from pysus.api.ftp.databases import *  # noqa


def get_version() -> str:
    try:
        return importlib_metadata.version(__name__)
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return "1.0.1"


version: str = get_version()
__version__: str = version
