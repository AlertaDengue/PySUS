"""
Download data from CIHA and CIH (Old)
Hospital and Ambulatorial information system
http://ciha.datasus.gov.br/CIHA/index.php?area=03

by fccoelho
license: GPL V3 or Later
"""
from typing import Union

from loguru import logger

from pysus.ftp.databases.ciha import CIHA
from pysus.ftp import CACHEPATH
from pysus.ftp.utils import parse_UFs

ciha = CIHA().load()


def get_available_years(
    states: Union[list, str] = None,
) -> dict[str:set[int]]:
    """
    Fetch available years for the `states`.
    :param states: UF code. E.g: "SP" or ["SP", "RJ"]
    :return: list of years in integers
    """
    ufs = parse_UFs(states)

    years = dict()
    for uf in ufs:
        files = ciha.get_files(uf=uf)
        years[uf] = set(sorted([ciha.describe(f)["year"] for f in files]))

    if len(set([len(v) for v in years.values()])) > 1:
        logger.warning(f"Distinct years were found for UFs: {years}")

    return sorted(list(set.intersection(*map(set, years.values()))))


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
