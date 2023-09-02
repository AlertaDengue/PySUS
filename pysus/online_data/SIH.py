"""
Downloads SIH data from Datasus FTP server
Created on 21/09/18
by fccoelho
license: GPL V3 or Later
"""
from typing import Union

from pysus.ftp import CACHEPATH
from pysus.ftp.databases import SIH

sih = SIH()


def download(
    states: Union[str, list],
    years: Union[str, list, int],
    months: Union[str, list, int],
    groups: Union[str, list],
    data_dir: str = CACHEPATH,
) -> list:
    """
    Download SIH records for state, year and month
    :param states: 2 letter state code, can be a list
    :param years: 4 digit integer, can be a list
    :param months: 1 to 12, can be a list
    :param groups: the groups of datasets to be downloaded.
                   See `sih.groups`
    :param data_dir: Directory where parquets will be downloaded.
    :return: list with the downloaded files
    """
    files = sih.get_files(
        groups=groups, ufs=states, months=months, years=years
    )
    downloaded = []
    for file in files:
        downloaded.append(file.download(local_dir=data_dir))
    return downloaded
