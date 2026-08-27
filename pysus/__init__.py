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
        return "2.10.5"  # changed by semantic-release"


version: str = get_version()
__version__: str = version

# Canonical __all__: everything from _impl plus the local names.
# Keep the old name ``info()`` as a convenience alias.
# ── Single import from the implementation layer ─────────────────
# This is the *only* import line that populates pysus.*.  Every
# user-facing function, class, and error lives in _impl.__all__.
from pysus.api._impl import *  # noqa: E402,F401,F403
from pysus.api._impl import __all__ as _impl_all  # noqa: E402,F401
from pysus.api._impl import info_table as info  # noqa: E402,F401

__all__ = [*_impl_all, "set_cache", "CACHEPATH"]  # type: ignore[has-type]


def _first_run_message() -> None:  # pragma: no cover
    """Print a friendly message on the first time PySUS is imported."""
    sentinel = CACHEPATH / ".pysus-seen"
    if sentinel.exists():
        return
    CACHEPATH.mkdir(parents=True, exist_ok=True)
    sentinel.touch(exist_ok=True)
    print(
        f"PySUS {version} -- welcome!\n"
        f"Data cache: {CACHEPATH}\n"
        f"Change it with: pysus.set_cache('/your/path')\n"
        f"Browse datasets with: pysus.info()\n"
    )


_first_run_message()
