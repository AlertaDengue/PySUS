"""PySUS Python package"""

import os
import pathlib
from importlib import metadata as importlib_metadata

CACHEPATH: pathlib.Path = pathlib.Path(
    os.getenv(
        "PYSUS_CACHEPATH",
        os.path.join(str(pathlib.Path.home()), "pysus"),
    )
)


def set_cache(path: str | pathlib.Path) -> pathlib.Path:
    """Set the global cache directory for PySUS.

    All downloaded files are stored under this path.  The default is
    ``~/pysus`` (or the ``PYSUS_CACHEPATH`` environment variable).

    Parameters
    ----------
    path : str or Path
        New cache directory.  Parent directories are created
        automatically.

    Returns
    -------
    Path
        The resolved cache path.

    Examples
    --------
    >>> from pysus import set_cache
    >>> set_cache("/data/pysus")
    PosixPath('/data/pysus')
    """
    global CACHEPATH  # noqa: PLW0603
    CACHEPATH = pathlib.Path(path).resolve()
    CACHEPATH.mkdir(parents=True, exist_ok=True)
    return CACHEPATH


def get_version() -> str:
    try:
        return importlib_metadata.version(__name__)
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return "2.9.0"  # changed by semantic-release"


version: str = get_version()
__version__: str = version

from pysus.api._impl.databases import *  # noqa
from pysus.api.client import PySUS  # noqa
