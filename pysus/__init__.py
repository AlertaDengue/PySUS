"""PySUS — Python interface to Brazilian public health datasets.

PySUS provides seamless access to Brazil's DATASUS and open-health data
repositories, covering disease notifications (SINAN), vital statistics
(SINASC, SIM), hospital admissions (SIH), ambulatory care (SIA),
immunisations (PNI), health facilities (CNES), and 18 Saude portal themes.

Two styles of public API
─────────────────────────

Flat (original)::

    import pysus
    pysus.sinan(disease="deng", year=2020, as_dataframe=True)

Origin-namespaced (recommended)::

    pysus.ftp.sinan(disease="deng", year=2020, as_dataframe=True)
    pysus.saude.arboviroses(as_dataframe=True)
    pysus.dadosgov.sinan(disease="deng", year=2020, as_dataframe=True)

The namespaced style makes the data source impossible to ignore, preventing
the class of bug where a call silently returns a different dataset snapshot
than the caller expected.

Origins
───────
- ``pysus.ftp``      — DATASUS FTP (S3 catalog mirror; 10 databases)
- ``pysus.dadosgov``  — dados.gov.br (CKAN portal; 6 databases)
- ``pysus.saude``     — dadosabertos.saude.gov.br (18 theme datasets)

Each namespace exposes the same interface: per-database fetchers, discovery
(``list_files``, ``info``), and metadata (``get_origin_meta``).

Quick-start
───────────
>>> import pysus
>>> pysus.info()                        # show all available datasets
>>> pysus.set_cache("/my/cache")        # change the download cache path
>>> df = pysus.sinan(disease="deng", year=2020, as_dataframe=True)

Origin namespace examples
─────────────────────────
>>> pysus.ftp.sih(state="RJ", year=2020, month=1, as_dataframe=True)
>>> pysus.dadosgov.sinasc(state="SP", year=2019, as_dataframe=True)
>>> pysus.saude.vacinacao(as_dataframe=True)

Getting help
────────────
>>> import pysus
>>> pysus.ftp.sinan?                    # help on a specific function
>>> pysus.ftp.info()                    # datasets for the FTP origin
>>> pysus.saude.get_origin_meta()       # metadata about the Saude origin
"""

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
        return "2.11.1"  # changed by semantic-release"


version: str = get_version()
__version__: str = version

# ── Origin namespaces ────────────────────────────────────────────
# ``pysus.ftp``, ``pysus.dadosgov``, ``pysus.saude`` expose origin-scoped
# fetchers.  Importing them registers the attributes on this package so
# ``pysus.ftp.sinan(...)`` works directly (importing the submodule also
# works for ``from pysus.ftp import sinan``).
from pysus import dadosgov, ftp, saude  # noqa: E402,F401

# Canonical __all__: everything from _impl plus the local names.
# Keep the old name ``info()`` as a convenience alias.
# ── Single import from the implementation layer ─────────────────
# This is the *only* import line that populates pysus.*.  Every
# user-facing function, class, and error lives in _impl.__all__.
from pysus.api._impl import *  # noqa: E402,F401,F403
from pysus.api._impl import __all__ as _impl_all  # noqa: E402,F401
from pysus.api._impl import info_table as info  # noqa: E402,F401

__all__ = [
    *_impl_all,  # type: ignore[has-type]
    "set_cache",
    "CACHEPATH",
    "ftp",
    "dadosgov",
    "saude",
]


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
