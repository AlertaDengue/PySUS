"""PySUS Python package"""

import os
import pathlib
from importlib import metadata as importlib_metadata
from typing import Final

CACHEPATH: Final[pathlib.Path] = pathlib.Path(
    os.getenv(
        "PYSUS_CACHEPATH",
        os.path.join(str(pathlib.Path.home()), "pysus"),
    )
)


def get_version() -> str:
    try:
        return importlib_metadata.version(__name__)
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return "1.0.1"  # changed by semantic-release"


version: str = get_version()
__version__: str = version

from pysus.api._impl.databases import *  # noqa
