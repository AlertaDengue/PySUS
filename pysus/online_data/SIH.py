"""
Downloads SIH data from Datasus FTP server
Created on 21/09/18
by fccoelho
license: GPL V3 or Later
"""
from typing import Union

from pysus.ftp import CACHEPATH
from pysus.ftp.databases.sih import SIH

sih = SIH().load()


def get_available_years(
    group: str,
    states: Union[str, list] = None,
    months: Union[str, list, int] = None,
) -> list:
    """
    Get SIH years for group and/or state and/or month and returns a list of years
    :param group:
        RD: AIH Reduzida
        RJ: AIH Rejeitada
        ER: AIH Rejeitada com erro
        SP: Serviços Profissionais
        CH: Cadastro Hospitalar
        CM: # TODO
    :param months: 1 to 12, can be a list of years. E.g.: 1 or [1, 2, 3]
    :param states: 2 letter uf code, can be a list. E.g: "SP" or ["SP", "RJ"]
    :return: list of available years
    """
    files = sih.get_files(group, uf=states, month=months)
    return sorted(list(set(sih.describe(f)["year"] for f in files)))


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
    :return: list with the downloaded files as ParquetData objects
    """
    files = sih.get_files(
        group=groups, uf=states, month=months, year=years
    )
    return sih.download(files, local_dir=data_dir)
