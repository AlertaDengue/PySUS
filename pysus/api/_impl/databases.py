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
from anyio import to_thread
from pysus.api.client import PySUS
from pysus.api.ducklake.catalog import CatalogDataset, CatalogFile, DatasetGroup
from pysus.api.types import State
from sqlalchemy.orm import joinedload
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
    async def _fetch():
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

    return asyncio.run(_fetch())


def sinan(
    disease: str,
    year: int | list[int],
    **kwargs,
) -> pd.DataFrame:
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
    return _fetch_data(dataset="ibge", group=group, year=year)


def cnes(
    state: State,
    year: int | list[int],
    month: int | list[int],
    group: str | None = None,
    **kwargs,
) -> pd.DataFrame:
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
    group: str | None = None,
    state: str | None = None,
    year: int | list[int] | None = None,
    month: int | list[int] | None = None,
    **kwargs,
) -> pd.DataFrame:
    async def _list():
        async with PySUS() as pysus:
            ducklake = await pysus.get_ducklake()
            if ducklake._Session is None:
                await ducklake.connect()

            def _query():
                with ducklake._Session() as session:
                    q = session.query(CatalogFile).options(
                        joinedload(CatalogFile.dataset),
                        joinedload(CatalogFile.group),
                    )

                    if dataset:
                        q = q.join(CatalogDataset).filter(
                            CatalogDataset.name == dataset.lower()
                        )

                    if group:
                        q = q.join(DatasetGroup).filter(
                            DatasetGroup.name == group
                        )

                    if state:
                        q = q.filter(CatalogFile.state == state.upper())

                    years = [year] if isinstance(year, int) else (year or [])
                    months = (
                        [month] if isinstance(month, int) else (month or [])
                    )

                    if years:
                        q = q.filter(CatalogFile.year.in_(years))
                    if months:
                        q = q.filter(CatalogFile.month.in_(months))

                    results = q.all()
                    session.expunge_all()
                    return results

            records = await to_thread.run_sync(_query)

            return [
                {
                    "name": r.path.split("/")[-1],
                    "path": r.path,
                    "dataset": r.dataset.name if r.dataset else None,
                    "group": r.group.name if r.group else None,
                    "year": r.year,
                    "month": r.month,
                    "state": r.state,
                    "modify": r.origin_modified,
                }
                for r in records
            ]

    return pd.DataFrame(asyncio.run(_list()))
