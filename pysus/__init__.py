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
from pysus.api.flatten import flatten_json_columns  # noqa: F401,E402
from pysus.api.mappings import to_english  # noqa: F401,E402
from pysus.api.progress import disable_progress_bars  # noqa: F401,E402
from pysus.api.progress import enable_progress_bars  # noqa: F401,E402

_FTP_DESCRIPTIONS: dict[str, str] = {
    "CIHA": "Hospital & ambulatory admission records",
    "CNES": "Health facility registry",
    "IBGEDATASUS": "Population & census data (IBGE)",
    "PNI": "National immunisation programme",
    "SIA": "Ambulatory care information system",
    "SIH": "Hospital admission information system",
    "SIM": "Mortality information system",
    "SINAN": "Notifiable disease information system",
    "SINASC": "Live birth information system",
}

_DADOSGOV_DESCRIPTIONS: dict[str, str] = {
    "CNES": "Health facility registry",
    "PNI": "National immunisation programme",
    "SIM": "Mortality information system",
    "SINAN": "Notifiable disease information system",
    "SINASC": "Live birth information system",
    "COVID19": "Confirmed COVID-19 cases",
}


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


def info() -> None:
    """Print a table of all available datasets and their origins.

    Shows dataset name, origin, whether authentication is required,
    and a short description.

    Examples
    --------
    >>> import pysus
    >>> pysus.info()
    """
    rows: list[dict[str, str]] = []

    # FTP datasets (anonymous)
    try:
        from pysus.api.ftp.databases import AVAILABLE_DATABASES

        for ds_cls in AVAILABLE_DATABASES:
            name = ds_cls.__name__
            desc = _FTP_DESCRIPTIONS.get(name, name)
            rows.append(
                {
                    "name": name,
                    "origin": "FTP",
                    "auth": "no",
                    "description": desc,
                },
            )
    except Exception:  # noqa: BLE001
        pass

    # Saude / OpenDataSUS datasets (anonymous)
    try:
        from pysus.api.saude.databases import DATASET_SPECS

        for spec in DATASET_SPECS:
            rows.append(
                {
                    "name": spec.name,
                    "origin": "Saude",
                    "auth": "no",
                    "description": spec.long_name,
                },
            )
    except Exception:  # noqa: BLE001
        pass

    # DadosGov datasets (token required)
    try:
        from pysus.api.dadosgov import databases as dg_databases

        for dg_cls in dg_databases.AVAILABLE_DATABASES:
            dg_name = dg_cls.__name__
            desc = _DADOSGOV_DESCRIPTIONS.get(dg_name, dg_name)
            rows.append(
                {
                    "name": dg_name,
                    "origin": "DadosGov",
                    "auth": "yes",
                    "description": desc,
                },
            )
    except Exception:  # noqa: BLE001
        pass

    if not rows:
        print("No datasets available.")
        return

    # Build aligned table
    name_w = max(len(r["name"]) for r in rows)
    origin_w = max(len(r["origin"]) for r in rows)
    auth_w = max(len(r["auth"]) for r in rows)

    header = (
        f"  {'Name':<{name_w}}  {'Origin':<{origin_w}}  "
        f"{'Auth':<{auth_w}}  Description"
    )
    sep = "  " + "-" * (len(header) - 2)

    print(sep)
    print(header)
    print(sep)
    for r in rows:
        print(
            f"  {r['name']:<{name_w}}  {r['origin']:<{origin_w}}  "
            f"{r['auth']:<{auth_w}}  {r['description']}",
        )
    print(sep)
    print(
        f"\n  Total: {len(rows)} datasets | Cache: {CACHEPATH}",
    )


_first_run_message()
