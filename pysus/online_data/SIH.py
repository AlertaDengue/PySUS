"""
Downloads SIH data from Datasus FTP server
Created on 21/09/18
by fccoelho
license: GPL V3 or Later
"""
from typing import Union

from pysus.online_data import CACHEPATH, FTP_Downloader


def download(
    states: Union[str, list],
    years: Union[str, list, int],
    months: Union[str, list, int],
    group: str = 'RD',
    data_dir: str = CACHEPATH,
) -> list:
    """
    Download SIH records for state, year and month
    :param months: 1 to 12, can be a list
    :param states: 2 letter state code, can be a list
    :param years: 4 digit integer, can be a list
    :param group: the group of datasets to be downloaded (accepts only one)
    :param data_dir: Directory where parquets will be downloaded.
    :return: the directory(ies) where parquets were downloaded
    """
    return FTP_Downloader('SIH').download(
        UFs=states,
        years=years,
        months=months,
        SIH_group=group,
        local_dir=data_dir,
    )
