"""
Download SINASC data from DATASUS FTP server
Created on 01/11/17
by fccoelho
license: GPL V3 or Later
"""
from typing import Union

from pysus.ftp import CACHEPATH
from pysus.ftp.databases.sinasc import SINASC

sinasc = SINASC().load()


def get_available_years(states):
    """
    Get SIH years for states
    :param states: 2 letter UF code, can be a list. E.g: "SP" or ["SP", "RJ"]
    :return: list of available years
    """
    files = sinasc.get_files(["DN", "DNR"], uf=states)
    return sorted(list(set(sinasc.describe(f)["year"] for f in files)))


def download(
    groups: Union[str, list],
    states: Union[str, list],
    years: Union[str, list, int],
    data_dir: str = CACHEPATH,
) -> list:
    """
    Downloads data directly from Datasus ftp server
    :param groups: either DN, DNR or both
    :param states: two-letter state identifier: MG == Minas Gerais,
                   can be a list
    :param years: years to download
    :return: list of downloaded files
    """
    files = sinasc.get_files(groups, uf=states, year=years)
    return sinasc.download(files, local_dir=data_dir)
