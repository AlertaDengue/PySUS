"""High-level convenience functions for fetching Brazilian health data.

Each function wraps an asynchronous query/download pipeline and returns a
pandas DataFrame.  The available datasets cover disease notification (SINAN),
vital statistics (SINASC, SIM), hospital admissions (SIH), ambulatory care
(SIA), immunisation (PNI), census data (IBGE), health facilities (CNES),
and hospitalisation records (CIHA).
"""

from __future__ import annotations

import asyncio
from typing import cast

import pandas as pd
from pysus.api import types
from tqdm.asyncio import tqdm

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


def _fetch_data(
    dataset: str,
    group: str | None = None,
    state: str | None = None,
    year: int | list[int] | None = None,
    month: int | list[int] | None = None,
    origin: str | None = None,
    columns: list[str] | None = None,
    show_progress: bool = True,
    as_dataframe: bool = False,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Query, download, and process Parquet files for a given dataset.

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
    origin : str, optional
        Restrict to a specific origin (``"FTP"``, ``"Saude"``,
        ``"DadosGov"``, ``"DuckLake"``).  ``None`` uses the DuckLake
        catalog which merges all origins.
    columns : list[str], optional
        Subset of column names to keep in the final DataFrame.
    show_progress : bool, optional
        Whether to display a tqdm progress bar during download.
    as_dataframe : bool, optional
        Whether to concatenate and return a pandas DataFrame.
    **kwargs
        Forwarded to :meth:`PySUS.read_parquet`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths to downloaded Parquet files (default) or a DataFrame.
    """

    async def _fetch() -> list[str] | pd.DataFrame:
        from pysus.api.client import PySUS
        from pysus.api.types import DADOSGOV, DUCKLAKE, FTP, SAUDE

        async with PySUS() as pysus:
            client_filter = None
            if origin is not None:
                origin_upper = origin.upper()
                mapping = {
                    "FTP": FTP,
                    "SAUDE": SAUDE,
                    "DUCKLAKE": DUCKLAKE,
                    "DADOSGOV": DADOSGOV,
                }
                client_filter = mapping.get(origin_upper)

            files = await pysus.query(
                client=client_filter,
                dataset=dataset,
                group=group,
                state=state,
                year=year,
                month=month,
            )

            if not files:
                if as_dataframe:
                    return pd.DataFrame()
                return cast(list[str], [])

            sem = asyncio.Semaphore(3)

            async def _throttled_download(f):
                async with sem:
                    return await pysus.download(f)

            tasks = [_throttled_download(f) for f in files]

            if show_progress:
                downloaded_files = await tqdm.gather(
                    *tasks,
                    desc=f"Downloading {dataset}",
                    unit="file",
                )
            else:
                downloaded_files = await asyncio.gather(*tasks)

            paths: list[str] = [str(f.path) for f in downloaded_files]

            if as_dataframe:
                res = pysus.read_parquet(paths, **kwargs)
                df = res.df() if not isinstance(res, pd.DataFrame) else res
                if columns:
                    df = df[[c for c in columns if c in df.columns]]
                return cast(pd.DataFrame, df)

            return paths

    from pysus.api.client import _run_sync

    return cast(list[str] | pd.DataFrame, _run_sync(_fetch()))


def sinan(
    disease: types.DatasetName,
    year: int | list[int],
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch SINAN records for a given disease and year(s).

    SINAN is the Brazilian notifiable-disease information system.

    Parameters
    ----------
    disease : str
        Disease code (e.g. ``"DENG"`` for dengue, ``"ZIKA"``).
    year : int | list[int]
        Year or list of years to fetch.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.sinan("DENG", 2020, as_dataframe=True)
    """
    return _fetch_data(
        dataset="sinan",
        group=disease.upper(),
        year=year,
        **kwargs,
    )


def sinasc(
    state: types.State,
    year: int | list[int],
    group: str | None = None,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch SINASC birth certificates for a given state, year(s).

    SINASC is the Brazilian live birth information system.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.sinasc("RJ", 2020, as_dataframe=True)
    """
    return _fetch_data(
        dataset="sinasc",
        state=state.upper(),
        group=group,
        year=year,
        **kwargs,
    )


def sim(
    state: types.State,
    year: int | list[int],
    group: str | None = None,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch SIM mortality records for a given state, year(s).

    SIM is the Brazilian mortality information system.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.sim("RJ", 2020, as_dataframe=True)
    """
    return _fetch_data(
        dataset="sim",
        state=state.upper(),
        group=group,
        year=year,
        **kwargs,
    )


def sih(
    state: types.State,
    year: int | list[int],
    month: int | list[int],
    group: str | None = None,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch SIH hospital admissions for a state, year, month.

    SIH is the Brazilian hospital admission information system.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    month : int | list[int]
        Month or list of months to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.sih("RJ", 2020, 1, as_dataframe=True)
    """
    return _fetch_data(
        dataset="sih",
        state=state.upper(),
        group=group,
        year=year,
        month=month,
        **kwargs,
    )


def sia(
    state: types.State,
    year: int | list[int],
    month: int | list[int],
    group: str | None = None,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch SIA ambulatory care for a state, year, month.

    SIA is the Brazilian ambulatory care information system.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    month : int | list[int]
        Month or list of months to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.sia("RJ", 2020, 1, as_dataframe=True)
    """
    return _fetch_data(
        dataset="sia",
        state=state.upper(),
        group=group,
        year=year,
        month=month,
        **kwargs,
    )


def pni(
    state: types.State,
    year: int | list[int],
    group: str | None = None,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch PNI immunisation records for a given state, year(s).

    PNI is the Brazilian national immunisation programme.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.pni("RJ", 2020, as_dataframe=True)
    """
    return _fetch_data(
        dataset="pni",
        state=state.upper(),
        group=group,
        year=year,
        **kwargs,
    )


def ibge(
    year: int | list[int],
    group: str | None = None,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch IBGE census data for given year(s).

    IBGE provides census and demographic data.

    Parameters
    ----------
    year : int | list[int]
        Year or list of years to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.ibge(2020, as_dataframe=True)
    """
    return _fetch_data(dataset="ibge", group=group, year=year, **kwargs)


def cnes(
    state: types.State,
    year: int | list[int],
    month: int | list[int],
    group: str | None = None,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch CNES health facilities for a state, year, month.

    CNES is the Brazilian registry of health-care facilities.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    month : int | list[int]
        Month or list of months to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.cnes("RJ", 2020, 1, as_dataframe=True)
    """
    return _fetch_data(
        dataset="cnes",
        state=state.upper(),
        group=group,
        year=year,
        month=month,
        **kwargs,
    )


def ciha(
    state: types.State,
    year: int | list[int],
    month: int | list[int],
    group: str | None = "CIHA",
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch CIHA hospitalisation records for state, year, month.

    CIHA provides hospitalisation records.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    month : int | list[int]
        Month or list of months to fetch.
    group : str, optional
        Grouping code.  Default is ``"CIHA"``.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.ciha("RJ", 2020, 1, as_dataframe=True)
    """
    return _fetch_data(
        dataset="ciha",
        state=state.upper(),
        group=group,
        year=year,
        month=month,
        **kwargs,
    )


def list_files(
    dataset: types.DatasetName,
    client: types.Origin | None = None,
    group: str | None = None,
    state: str | None = None,
    year: int | list[int] | None = None,
    month: int | list[int] | None = None,
    **kwargs,
) -> pd.DataFrame:
    """List catalog files filtered by client, group, state, year, and month.

    Queries the PySUS API metadata and returns a DataFrame with file data
    without downloading the actual files.

    Parameters
    ----------
    dataset : str
        Dataset name (e.g. ``"SINAN"``).
    client : str, optional
        Data source client to query.
    group : str, optional
        Group or disease code to filter by.
    state : str, optional
        Two-letter state abbreviation.
    year : int | list[int], optional
        Year or list of years to filter by.
    month : int | list[int], optional
        Month or list of months to filter by.
    **kwargs
        Forwarded to :meth:`PySUS.query`.

    Returns
    -------
    pd.DataFrame
        Columns: name, path, dataset, group, year, month, state, modify.

    Examples
    --------
    >>> pysus.list_files("SINAN", year=2020, state="RJ")
    """

    async def _list():
        from pysus.api.client import PySUS

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
                    "dataset": (r.dataset.name if r.dataset else None),
                    "group": (r.group.name if r.group else None),
                    "year": r.record.year,
                    "month": r.record.month,
                    "state": r.record.state,
                    "modify": r.record.origin_modified,
                }
                for r in records
            ]

    return pd.DataFrame(asyncio.run(_list()))
