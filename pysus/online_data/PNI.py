"""
Download data from the national immunization program
"""
from typing import Union

from pysus.online_data import FTP_Downloader, FTP_Inspect
from pysus.ftp import CACHEPATH


def download(
    states: Union[str, list],
    years: Union[str, list, int],
    data_dir: str = CACHEPATH,
) -> list:
    """
    Download imunization records for a given States and years.
    :param state: uf two letter code, can be a list
    :param year: year in 4 digits, can be a list
    :param data_dir: directory where data will be downloaded
    :return: list of downloaded parquet paths
    """
    return FTP_Downloader('PNI').download(
        PNI_group='CPNI', UFs=states, years=years, local_dir=data_dir
    )


def get_available_years(state):
    """
    Fetch available years (dbf names) for the `state`.
    :param state: uf code
    :return: list of strings (filenames)
    """
    return FTP_Inspect('PNI').list_available_years(UF=state, PNI_group='CPNI')


def available_docs():
    return FTP_Inspect('PNI').list_all(PNI_group='CPNI')
