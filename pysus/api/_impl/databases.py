"""High-level convenience functions for fetching Brazilian health data.

Each function wraps an asynchronous query/download pipeline and returns a
pandas DataFrame.  The available datasets cover disease notification (SINAN),
vital statistics (SINASC, SIM), hospital admissions (SIH), ambulatory care
(SIA), immunisation (PNI), census data (IBGE), health facilities (CNES),
and hospitalisation records (CIHA).
"""

__all__ = [
    "sinan",
    "sinasc",
    "sim",
    "sih",
    "sia",
    "pni",
    "ibge",
    "cnes",
    "ciha",
    "list_files",
]

import asyncio
from typing import Literal

import pandas as pd
from pysus.api.client import PySUS
from pysus.api.types import State
from tqdm import tqdm


def _fetch_data(
    dataset: str,
    group: str | None = None,
    state: str | None = None,
    year: int | list[int] | None = None,
    month: int | list[int] | None = None,
    show_progress: bool = True,
    **kwargs,
) -> pd.DataFrame:
    """Query, download, and concatenate Parquet files for a given dataset.

    Internally creates an async event loop, queries the PySUS API for matching
    files, downloads them, and reads them into a single DataFrame.

    Parameters
    ----------
    dataset : str
        Name of the dataset (e.g. ``"sinan"``, ``"sinasc"``).
    group : str, optional
        Group or disease code to filter by.
    state : str, optional
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int], optional
        Year or list of years to fetch.
    month : int | list[int], optional
        Month or list of months to fetch.
    show_progress : bool, optional
        Whether to display a tqdm progress bar during download.  Default is
        ``True``.
    **kwargs
        Additional arguments forwarded to :meth:`PySUS.read_parquet`.

    Returns
    -------
    pd.DataFrame
        Concatenated data from all matching Parquet files.  Returns an empty
        DataFrame when no files are found.

    Raises
    ------
    RuntimeError
        If an event loop is already running but ``nest_asyncio`` is not
        installed.
    """

    async def _fetch():
        """Coroutine that performs the actual API query, download, and read."""

        async with PySUS() as pysus:
            years = [year] if isinstance(year, int) else (year or [None])
            months = [month] if isinstance(month, int) else (month or [None])

            files = []
            for y in years:
                for m in months:
                    files.extend(
                        await pysus.query(
                            dataset=dataset,
                            group=group,
                            state=state,
                            year=y,
                            month=m,
                        )
                    )

            paths = []
            if show_progress:
                for file in tqdm(
                    files,
                    desc=f"Downloading {dataset}",
                    unit="file",
                ):
                    f = await pysus.download(file)
                    paths.append(f.path)
            else:
                for file in files:
                    f = await pysus.download(file)
                    paths.append(f.path)

            return (
                pysus.read_parquet(
                    paths,
                    **kwargs,
                ).df()
                if paths
                else pd.DataFrame()
            )

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        try:
            import nest_asyncio  # noqa: PLC0415

            nest_asyncio.apply()
        except ImportError:
            msg = (
                "nest_asyncio is required when running inside Jupyter. "
                "Install it with: pip install nest_asyncio"
            )
            raise RuntimeError(msg) from None
        return loop.run_until_complete(_fetch())
    else:
        return asyncio.run(_fetch())


def sinan(
    disease: Literal[
        "ACBI",
        "ACGR",
        "ANIM",
        "ANTR",
        "BOTU",
        "CANC",
        "CHAG",
        "CHIK",
        "COLE",
        "COQU",
        "DENG",
        "DERM",
        "DIFT",
        "ESQU",
        "EXAN",
        "FMAC",
        "FTIF",
        "HANS",
        "HANT",
        "HEPA",
        "IEXO",
        "INFL",
        "LEIV",
        "LEPT",
        "LERD",
        "LTAN",
        "MALA",
        "MENI",
        "MENT",
        "NTRA",
        "PAIR",
        "PEST",
        "PFAN",
        "PNEU",
        "RAIV",
        "SDTA",
        "SIFA",
        "SIFC",
        "SIFG",
        "SRC",
        "TETA",
        "TETN",
        "TOXC",
        "TOXG",
        "TRAC",
        "TUBE",
        "VARC",
        "VIOL",
        "ZIKA",
    ],
    year: int | list[int],
    **kwargs,
) -> pd.DataFrame:
    """Fetch SINAN records for a given disease and year(s).

    SINAN (Sistema de Informação de Agravos de Notificação) is the Brazilian
    notifiable-disease information system.

    Parameters
    ----------
    disease : Literal
        Disease code (e.g. ``"DENG"`` for dengue, ``"ZIKA"`` for zika).
    year : int | list[int]
        Year or list of years to fetch.
    **kwargs
        Additional arguments forwarded to :func:`_fetch_data`.

    Returns
    -------
    pd.DataFrame
        SINAN records for the specified disease and year(s).
    """
    return _fetch_data(
        dataset="sinan",
        group=disease.upper(),
        year=year,
    )


def sinasc(
    state: State,
    year: int | list[int],
    group: str | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Fetch SINASC birth certificates for a given state, year(s), and group.

    SINASC (Sistema de Informação sobre Nascidos Vivos) is the Brazilian live
    birth information system.

    Parameters
    ----------
    state : State
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Additional arguments forwarded to :func:`_fetch_data`.

    Returns
    -------
    pd.DataFrame
        SINASC birth records for the specified state, year(s), and group.
    """
    return _fetch_data(
        dataset="sinasc",
        state=state.upper(),
        group=group,
        year=year,
    )


def sim(
    state: State,
    year: int | list[int],
    group: str | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Fetch SIM mortality records for a given state, year(s), and group.

    SIM (Sistema de Informação sobre Mortalidade) is the Brazilian mortality
    information system.

    Parameters
    ----------
    state : State
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Additional arguments forwarded to :func:`_fetch_data`.

    Returns
    -------
    pd.DataFrame
        SIM mortality records for the specified state, year(s), and group.
    """
    return _fetch_data(
        dataset="sim",
        state=state.upper(),
        group=group,
        year=year,
    )


def sih(
    state: State,
    year: int | list[int],
    month: int | list[int],
    group: str | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Fetch SIH hospital admissions for a state, year, month, and group.

    SIH (Sistema de Informação Hospitalar) is the Brazilian hospital
    admission information system.

    Parameters
    ----------
    state : State
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    month : int | list[int]
        Month or list of months to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Additional arguments forwarded to :func:`_fetch_data`.

    Returns
    -------
    pd.DataFrame
        SIH hospital admission records.
    """
    return _fetch_data(
        dataset="sih",
        state=state.upper(),
        group=group,
        year=year,
        month=month,
    )


def sia(
    state: State,
    year: int | list[int],
    month: int | list[int],
    group: str | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Fetch SIA ambulatory care for a state, year, month, and group.

    SIA (Sistema de Informação Ambulatorial) is the Brazilian ambulatory care
    information system.

    Parameters
    ----------
    state : State
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    month : int | list[int]
        Month or list of months to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Additional arguments forwarded to :func:`_fetch_data`.

    Returns
    -------
    pd.DataFrame
        SIA ambulatory care records.
    """
    return _fetch_data(
        dataset="sia",
        state=state.upper(),
        group=group,
        year=year,
        month=month,
    )


def pni(
    state: State,
    year: int | list[int],
    group: str | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Fetch PNI immunisation records for a given state, year(s), and group.

    PNI (Programa Nacional de Imunizações) is the Brazilian national
    immunisation programme.

    Parameters
    ----------
    state : State
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Additional arguments forwarded to :func:`_fetch_data`.

    Returns
    -------
    pd.DataFrame
        PNI immunisation records.
    """
    return _fetch_data(
        dataset="pni",
        state=state.upper(),
        group=group,
        year=year,
    )


def ibge(
    year: int | list[int],
    group: str | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Fetch IBGE census data for given year(s) and optional group.

    IBGE (Instituto Brasileiro de Geografia e Estatística) provides census
    and demographic data.

    Parameters
    ----------
    year : int | list[int]
        Year or list of years to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Additional arguments forwarded to :func:`_fetch_data`.

    Returns
    -------
    pd.DataFrame
        IBGE census data for the specified year(s) and group.
    """
    return _fetch_data(dataset="ibge", group=group, year=year)


def cnes(
    state: State,
    year: int | list[int],
    month: int | list[int],
    group: str | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Fetch CNES health facilities for a state, year, month, and group.

    CNES (Cadastro Nacional de Estabelecimentos de Saúde) is the Brazilian
    registry of health-care facilities.

    Parameters
    ----------
    state : State
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    month : int | list[int]
        Month or list of months to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Additional arguments forwarded to :func:`_fetch_data`.

    Returns
    -------
    pd.DataFrame
        CNES health-facility records.
    """
    return _fetch_data(
        dataset="cnes",
        state=state.upper(),
        group=group,
        year=year,
        month=month,
    )


def ciha(
    state: State,
    year: int | list[int],
    month: int | list[int],
    group: str | None = "CIHA",
    **kwargs,
) -> pd.DataFrame:
    """Fetch CIHA hospitalisation records for state, year, month, and group.

    CIHA (Comunicação de Internação Hospitalar) provides hospitalisation
    records.

    Parameters
    ----------
    state : State
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    month : int | list[int]
        Month or list of months to fetch.
    group : str, optional
        Additional grouping code.  Default is ``"CIHA"``.
    ``**kwargs``
        Additional arguments forwarded to :func:`_fetch_data`.

    Returns
    -------
    pd.DataFrame
        CIHA hospitalisation records.
    """
    return _fetch_data(
        dataset="ciha",
        state=state.upper(),
        group=group,
        year=year,
        month=month,
    )


def list_files(
    dataset: Literal[
        "SINAN",
        "SINASC",
        "SIM",
        "SIH",
        "SIA",
        "PNI",
        "IBGE",
        "CNES",
        "CIHA",
    ],
    client: Literal["FTP", "DadosGov"] | None = None,
    group: str | None = None,
    state: str | None = None,
    year: int | list[int] | None = None,
    month: int | list[int] | None = None,
    **kwargs,
) -> pd.DataFrame:
    """List catalog files filtered by client, group, state, year, and month.

    Queries the PySUS API metadata and returns a DataFrame with file name,
    path, dataset, group, year, month, state, and last-modified timestamp for
    every matching file without downloading the actual data.

    Parameters
    ----------
    dataset : Literal
        Dataset name (e.g. ``"SINAN"``, ``"SINASC"``, etc.).
    client : Literal["FTP", "DadosGov"], optional
        Data source client to query.
    group : str, optional
        Group or disease code to filter by.
    state : str, optional
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int], optional
        Year or list of years to filter by.
    month : int | list[int], optional
        Month or list of months to filter by.
    **kwargs
        Additional arguments forwarded to :meth:`PySUS.query`.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ``name``, ``path``, ``dataset``, ``group``,
        ``year``, ``month``, ``state``, and ``modify``.
    """

    async def _list():
        """Coroutine that queries the PySUS API and builds the file list."""

        async with PySUS() as pysus:
            years = [year] if isinstance(year, int) else (year or [None])
            months = [month] if isinstance(month, int) else (month or [None])

            records = []
            for y in years:
                for m in months:
                    records.extend(
                        await pysus.query(
                            client=client,
                            dataset=dataset,
                            group=group,
                            state=state,
                            year=y,
                            month=m,
                        )
                    )

            return [
                {
                    "name": str(r.path).split("/")[-1],
                    "path": str(r.path),
                    "dataset": r.dataset.name if r.dataset else None,
                    "group": r.group.name if r.group else None,
                    "year": r.record.year,
                    "month": r.record.month,
                    "state": r.record.state,
                    "modify": r.record.origin_modified,
                }
                for r in records
            ]

    return pd.DataFrame(asyncio.run(_list()))
