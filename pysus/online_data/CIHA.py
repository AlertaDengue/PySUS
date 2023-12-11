"""
Download data from CIHA and CIH (Old)
Hospital and Ambulatorial information system
http://ciha.datasus.gov.br/CIHA/index.php?area=03

by fccoelho
license: GPL V3 or Later
"""
from typing import Union

from pysus.ftp.databases.ciha import CIHA
from pysus.ftp import CACHEPATH

ciha = CIHA().load()


def get_available_years(
    states: Union[list, str] = None,
    months: Union[str, int, list] = None
) -> dict[str:set[int]]:
    """
    Fetch available years for the `states` and/or `months`.
    :param states: UF code. E.g: "SP" or ["SP", "RJ"]
    :param months: month or months, 2 digits. E.g.: 1 or [1, 2]
    :return: list of years in integers
    """

    files = ciha.get_files(uf=states, month=months)
    return sorted(list(set([ciha.describe(f)["year"] for f in files])))


def download(
    states: Union[str, list],
    years: Union[str, list, int],
    months: Union[str, list, int],
    data_dir: str = CACHEPATH,
) -> list:
    """
    Download CIHA records for state, year and month and returns the Parquets
    files as a list of PartquetData
    :param months: 1 to 12, can be a list
    :param states: 2 letter state code,
    :param years: 4 digit integer
    """

    files = ciha.get_files(uf=states, year=years, month=months)
    return ciha.download(files, local_dir=data_dir)
