"""
Downloads SIA data from Datasus FTP server
Created on 21/09/18
by fccoelho
Modified on 22/11/22
by bcbernardo
license: GPL V3 or Later
"""
from pprint import pprint
from typing import Dict, Tuple, Union
from pysus.online_data import FTP_Downloader, CACHEPATH


group_dict: Dict[str, Tuple[str, int, int]] = {
    "PA": ("Produção Ambulatorial", 7, 1994),
    "BI": ("Boletim de Produção Ambulatorial individualizado", 1, 2008),
    "AD": ("APAC de Laudos Diversos", 1, 2008),
    "AM": ("APAC de Medicamentos", 1, 2008),
    "AN": ("APAC de Nefrologia", 1, 2008),
    "AQ": ("APAC de Quimioterapia", 1, 2008),
    "AR": ("APAC de Radioterapia", 1, 2008),
    "AB": ("APAC de Cirurgia Bariátrica", 1, 2008),
    "ACF": ("APAC de Confecção de Fístula", 1, 2008),
    "ATD": ("APAC de Tratamento Dialítico", 1, 2008),
    "AMP": ("APAC de Acompanhamento Multiprofissional", 1, 2008),
    "SAD": ("RAAS de Atenção Domiciliar", 1, 2008),
    "PS": ("RAAS Psicossocial", 1, 2008),
}

def show_datatypes():
    pprint(group_dict)

def download(
        states: Union[str, list],
        years: Union[str, list, int],
        months: Union[str, list, int],
        data_dir: str=CACHEPATH,
        group:str = "PA",
) -> list:
    """
    Download SIASUS records for state year and month and returns dataframe
    :param months: 1 to 12, can be a list
    :param states: 2 letter state code, can be a list
    :param years: 4 digit integer, can be a list
    :param data_dir: whether to cache files locally. default is True
    :param group: 2-3 letter document code, defaults to ['PA', 'BI']. 
    Codes should be one of the following:
        PA - Produção Ambulatorial
        BI - Boletim de Produção Ambulatorial individualizado
        AD - APAC de Laudos Diversos
        AM - APAC de Medicamentos
        AN - APAC de Nefrologia
        AQ - APAC de Quimioterapia
        AR - APAC de Radioterapia
        AB - APAC de Cirurgia Bariátrica
        ACF - APAC de Confecção de Fístula
        ATD - APAC de Tratamento Dialítico
        AMP - APAC de Acompanhamento Multiprofissional
        SAD - RAAS de Atenção Domiciliar
        PS - RAAS Psicossocial
    :return: list of downloaded parquet paths
    """
    return FTP_Downloader('SIA').download(
        UFs=states,
        years=years,
        months=months,
        local_dir=data_dir,
        SIA_group=group
    )
