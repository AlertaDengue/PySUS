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


def get_available_years(state):
    years = list(set([f.name[-2:] for f in sinasc.files]))
    files = set(sinasc.get_files(["DN", "DNR"], uf=state, year=years))

    def sort_year(file):
        _, year = sinasc.format(file)
        return int(year)

    return sorted(files, key=sort_year)
