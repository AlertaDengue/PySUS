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
]

import asyncio
import pandas as pd

from tqdm import tqdm
from pysus.api.client import PySUS
from pysus.api.types import State


def _fetch_data(
    dataset: str,
    group: str | None = None,
    state: str | None = None,
    year: int | list[int] | None = None,
    month: int | list[int] | None = None,
    group_filter: str | None = None,
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
                            group=group_filter or group,
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
        group_filter=disease.upper(),
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

