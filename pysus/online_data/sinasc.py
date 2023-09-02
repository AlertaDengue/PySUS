"""
Download SINASC data from DATASUS FTP server
Created on 01/11/17
by fccoelho
license: GPL V3 or Later
"""
from typing import Union

from pysus.online_data import CACHEPATH
from pysus.ftp.databases import SINASC

sinasc = SINASC()


def download(
    states: Union[str, list],
    years: Union[str, list, int],
    data_dir: str = CACHEPATH,
) -> list:
    """
    Downloads data directly from Datasus ftp server
    :param states: two-letter state identifier: MG == Minas Gerais,
                   can be a list
    :param years: years to download
    :return: list of downloaded files
    """
    files = sinasc.get_files(ufs=states, years=years)
    downloaded = []

    for file in files:
        downloaded.append(file.download(local_dir=data_dir))

    return downloaded


def get_available_years(state):
    years = list(set([f.name[-2:] for f in sinasc.files]))
    files = set(sinasc.get_files(ufs=state, years=years))

    def sort_year(file):
        _, year = sinasc.format(file)
        return int(year)

    return sorted(files, key=sort_year)
