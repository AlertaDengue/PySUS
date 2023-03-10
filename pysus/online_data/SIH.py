"""
Downloads SIH data from Datasus FTP server
Created on 21/09/18
by fccoelho
license: GPL V3 or Later
"""
from typing import Union
from pysus.online_data import CACHEPATH, FTP_Downloader


def download(states: Union[str, list], years: Union[str, list, int], months: Union[str, list, int], data_dir: str=CACHEPATH) -> list:
    """
    Download SIH records for state year and month and returns dataframe
    :param months: 1 to 12, can be a list
    :param states: 2 letter state code, can be alist
    :param years: 4 digit integer, can be a list
    :param data_dir: Directory where parquets will be downloaded.
    :return: a list of parquet paths
    """
    return FTP_Downloader('SIH').download(
        UFs=states,
        years=years,
        months=months,
        SIH_group='RD',
        local_dir=data_dir
    )
