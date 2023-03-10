from typing import Union
from pysus.online_data import FTP_Downloader, CACHEPATH

group_dict = {
    "LT": ["Leitos - A partir de Out/2005", 10, 2005],
    "ST": ["Estabelecimentos - A partir de Ago/2005", 8, 2005],
    "DC": ["Dados Complementares - A partir de Ago/2005", 8, 2005],
    "EQ": ["Equipamentos - A partir de Ago/2005", 8, 2005],
    "SR": ["Serviço Especializado - A partir de Ago/2005", 8, 2005],
    "HB": ["Habilitação - A partir de Mar/2007", 3, 2007],
    "PF": ["Profissional - A partir de Ago/2005", 8, 2005],
    "EP": ["Equipes - A partir de Abr/2007", 5, 2007],
    "IN": ["Incentivos - A partir de Nov/2007", 11, 2007],
    "RC": ["Regra Contratual - A partir de Mar/2007", 3, 2007],
    "EE": ["Estabelecimento de Ensino - A partir de Mar/2007", 3, 2007],
    "EF": ["Estabelecimento Filantrópico - A partir de Mar/2007", 3, 2007],
    "GM": ["Gestão e Metas - A partir de Jun/2007", 6, 2007],
}


def download(
        group: str, states: Union[str, list], years: Union[str, list, int], months: Union[str, list, int], data_dir: str=CACHEPATH
) -> list:
    """
    Download CNES records for group, state, year and month and returns a 
    list of local parquet files
    :param group:
        LT – Leitos - A partir de Out/2005
        ST – Estabelecimentos - A partir de Ago/2005
        DC - Dados Complementares - A partir de Ago/2005
        EQ – Equipamentos - A partir de Ago/2005
        SR - Serviço Especializado - A partir de Ago/2005
        HB – Habilitação - A partir de Mar/2007
        PF – Profissional - A partir de Ago/2005
        EP – Equipes - A partir de Abr/2007
        IN – Incentivos - A partir de Nov/2007
        RC - Regra Contratual - A partir de Mar/2007
        EE - Estabelecimento de Ensino - A partir de Mar/2007
        EF - Estabelecimento Filantrópico - A partir de Mar/2007
        GM - Gestão e Metas - A partir de Jun/2007
    :param months: 1 to 12, can be a list of years
    :param states: 2 letter state code, can be a list of UFs
    :param years: 4 digit integer, can be a list of years
    """
    return FTP_Downloader('CNES').download(
        CNES_group=group,
        UFs=states,
        years=years,
        months=months,
        local_dir=data_dir,
    )
