"""
Download data from the national immunization program
"""
from typing import Union, Literal

from loguru import logger

from pysus.ftp.databases.pni import PNI
from pysus.ftp import CACHEPATH
from pysus.ftp.utils import parse_UFs


pni = PNI().load()


def get_available_years(group, states):
    """
    Fetch available years for `group` and/or `months`.
    :param group: PNI group, options are "CPNI" or "DPNI"
    :param state: UF code, can be a list. E.g: "SP" or ["SP", "RJ"]
    :return: list of available years
    """
    ufs = parse_UFs(states)

    years = dict()
    for uf in ufs:
        files = pni.get_files(group, uf=uf)
        years[uf] = set(sorted([pni.describe(f)["year"] for f in files]))

    if len(set([len(v) for v in years.values()])) > 1:
        logger.warning(f"Distinct years were found for UFs: {years}")

    return sorted(list(set.intersection(*map(set, years.values()))))


def download(
    group: Union[list, Literal["CNPI", "DPNI"]],
    states: Union[str, list],
    years: Union[str, list, int],
    data_dir: str = CACHEPATH,
) -> list:
    """
    Download imunization records for a given States and years.
    :param group: PNI group, options are "CPNI" or "DPNI"
    :param state: uf two letter code, can be a list. E.g: "SP" or ["SP", "RJ"]
    :param year: year in 4 digits, can be a list. E.g: 1 or [1, 2, 3]
    :param data_dir: directory where data will be downloaded
    :return: list of downloaded ParquetData
    """
    files = pni.get_files(group, uf=states, year=years)
    return pni.download(files, local_dir=data_dir)
