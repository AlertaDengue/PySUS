import os
from datetime import datetime
from ftplib import FTP, error_perm

import pandas as pd
from dbfread import DBF

from pysus.online_data import CACHEPATH
from pysus.utilities.readdbc import read_dbc_geopandas

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
    group: str, state: str, year: int, month: int, cache: bool = True
) -> pd.DataFrame:
    """
    Download CNES records for group, state, year and month and returns dataframe
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
    :param month: 1 to 12
    :param state: 2 letter state code
    :param year: 4 digit integer
    """
    state = state.upper()
    assert len(str(year)) == 4
    year2 = str(year)[-2:]
    month = str(month).zfill(2)
    input_date = datetime(int(year), int(month), 1)
    avaiable_date = datetime(group_dict[group][2], group_dict[group][1], 1)
    if input_date < avaiable_date:
        raise ValueError(f"CNES does not contain data for {input_date}")
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    if input_date >= avaiable_date:
        ftype = "DBC"
        ftp.cwd("dissemin/publicos/CNES/200508_/Dados/{}/".format(group))
        fname = "{}{}{}{}.dbc".format(group, state, str(year2).zfill(2), month)
    cachefile = os.path.join(CACHEPATH, "CNES_" + fname.split(".")[0] + "_.parquet")
    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df
    df = _fetch_file(fname, ftp, ftype)
    if cache:
        df.to_parquet(cachefile)
    return df


def _fetch_file(fname: str, ftp: FTP, ftype: str)-> pd.DataFrame:
    try:
        ftp.retrbinary("RETR {}".format(fname), open(fname, "wb").write)
    except error_perm:
        raise Exception("File {} not available".format(fname))
    if ftype == "DBC":
        df = read_dbc_geopandas(fname, encoding="iso-8859-1")
    elif ftype == "DBF":
        dbf = DBF(fname, encoding="iso-8859-1")
        df = pd.DataFrame(list(dbf))
    os.unlink(fname)
    return df
