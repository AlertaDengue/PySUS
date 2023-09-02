"""
Download data from CIHA and CIH (Old)
Hospital and Ambulatorial information system
http://ciha.datasus.gov.br/CIHA/index.php?area=03

by fccoelho
license: GPL V3 or Later
"""
from typing import Union

from pysus.online_data import FTP_Downloader
from pysus.ftp import CACHEPATH


def download(
    states: Union[str, list],
    years: Union[str, list, int],
    months: Union[str, list, int],
    data_dir: str = CACHEPATH,
) -> list:
    """
    Download CIHA records for state, year and month and returns dataframe
    :param months: 1 to 12, can be a list
    :param states: 2 letter state code,
    :param years: 4 digit integer
    """
    return FTP_Downloader('CIHA').download(
        UFs=states,
        years=years,
        months=months,
        local_dir=data_dir,
    )
