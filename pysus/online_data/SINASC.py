"""
Download SINASC data from DATASUS FTP server
Created on 01/11/17
by fccoelho
license: GPL V3 or Later
"""
from typing import Union

from loguru import logger

from pysus.ftp import CACHEPATH
from pysus.ftp.databases.sinasc import SINASC
from pysus.ftp.utils import parse_UFs

sinasc = SINASC().load()


def get_available_years(group: str, states: Union[str, list[str]]) -> list:
    """
    Get SINASC years for states
    :param group:
        "DN": "Declarações de Nascidos Vivos",
        "DNR": "Dados dos Nascidos Vivos por UF de residência",
    :param states: 2 letter UF code, can be a list. E.g: "SP" or ["SP", "RJ"]
    :return: list of available years
    """
    ufs = parse_UFs(states)

    years = dict()
    for uf in ufs:
        files = sinasc.get_files(group, uf=uf)
        years[uf] = set(sorted([sinasc.describe(f)["year"] for f in files]))

    if len(set([len(v) for v in years.values()])) > 1:
        logger.warning(f"Distinct years were found for UFs: {years}")

    return sorted(list(set.intersection(*map(set, years.values()))))


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
