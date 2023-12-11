"""
Download data from the national immunization program
"""
from typing import Union, Literal

from pysus.ftp.databases.pni import PNI
from pysus.ftp import CACHEPATH


pni = PNI().load()


def get_available_years(group, states):
    """
    Fetch available years for `group` and/or `months`.
    :param group: PNI group, options are "CPNI" or "DPNI"
    :param state: UF code, can be a list. E.g: "SP" or ["SP", "RJ"]
    :return: list of available years
    """
    files = pni.get_files(group=group, uf=states)
    return sorted(list(set(pni.describe(f)["year"] for f in files)))


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
