"""
Downloads SIH data from Datasus FTP server
Created on 21/09/18
by fccoelho
license: GPL V3 or Later
"""

import os
from ftplib import FTP

import pandas as pd
from dbfread import DBF

from pysus.online_data import CACHEPATH
from pysus.utilities.readdbc import read_dbc


def download(state: str, year: int, month: int, cache: bool = True) -> object:
    """
    Download SIH records for state year and month and returns dataframe
    :param month: 1 to 12
    :param state: 2 letter state code
    :param year: 4 digit integer
    :param cache: Whether to cache or not. defaults to True.
    :return:
    """
    state = state.upper()
    year2 = int(str(year)[-2:])
    year2 = str(year2).zfill(2)
    month = str(month).zfill(2)
    if year < 1992:
        raise ValueError("SIH does not contain data before 1994")
    if year < 2008:
        ftype = "DBC"
        path = "/dissemin/publicos/SIHSUS/199201_200712/Dados"
        fname = f"RD{state}{year2}{month}.dbc"
    if year >= 2008:
        ftype = "DBC"
        path = f"/dissemin/publicos/SIHSUS/200801_/Dados"
        fname = f"RD{state}{year2}{month}.dbc"
    cachefile = os.path.join(CACHEPATH, "SIH_" + fname.split(".")[0] + "_.parquet")
    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df

    df = _fetch_file(fname, path, ftype)
    if cache:
        df.to_parquet(cachefile)
    return df


def _fetch_file(fname, path, ftype):
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    ftp.cwd(path)
    try:
        ftp.retrbinary("RETR {}".format(fname), open(fname, "wb").write)
    except:
        raise Exception("File {} not available".format(fname))
    if ftype == "DBC":
        df = read_dbc(fname, encoding="iso-8859-1")
    elif ftype == "DBF":
        dbf = DBF(fname, encoding="iso-8859-1")
        df = pd.DataFrame(list(dbf))
    os.unlink(fname)
    return df
