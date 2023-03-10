"""
Download SINASC data from DATASUS FTP server
Created on 01/11/17
by fccoelho
license: GPL V3 or Later
"""
from typing import Union
from pysus.online_data import FTP_Downloader, FTP_Inspect,CACHEPATH


def download(states: Union[str, list], years: Union[str, list, int], data_dir:str=CACHEPATH) -> list:
    """
    Downloads data directly from Datasus ftp server
    :param state: two-letter state identifier: MG == Minas Gerais,
                  can be a list
    :param year: 4 digit integer, can be a list
    :return: list of downloaded parquet paths
    """
    return FTP_Downloader('SINASC').download(
        UFs=states,
        years=years,
        local_dir=data_dir
    )


def get_available_years(state):
    return FTP_Inspect('SINASC').list_available_years(UF=state)
