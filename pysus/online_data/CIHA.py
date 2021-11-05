u"""
Download data from CIHA and CIH (Old)
Hospital and Ambulatorial information system
http://ciha.datasus.gov.br/CIHA/index.php?area=03

Created on 12/12/18
by fccoelho
license: GPL V3 or Later
"""

import os
from ftplib import FTP, error_perm

import pandas as pd
from dbfread import DBF

from pysus.online_data import CACHEPATH
from pysus.utilities.readdbc import read_dbc


def download(state: str, year: int, month: int, cache: bool = True) -> object:
    """
    Download CIHA records for state, year and month and returns dataframe
    :param month: 1 to 12
    :param state: 2 letter state code
    :param year: 4 digit integer
    """
    state = state.upper()
    year2 = str(year)[-2:]
    month = str(month).zfill(2)
    if year < 2008:
        raise ValueError("CIHA does not contain data before 2008")
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    if year > 2008 and year < 2011:
        ftype = "DBC"
        ftp.cwd("/dissemin/publicos/CIH/200801_201012/Dados")
        fname = "CR{}{}{}.dbc".format(state, year2, month)
    if year >= 2011:
        ftype = "DBC"
        ftp.cwd("/dissemin/publicos/CIHA/201101_/Dados".format(year))
        fname = "CIHA{}{}{}.dbc".format(state, str(year2).zfill(2), month)
    cachefile = os.path.join(CACHEPATH, "CIHA_" + fname.split(".")[0] + "_.parquet")
    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df
    df = _fetch_file(fname, ftp, ftype)
    if cache:
        df.to_parquet(cachefile)
    return df


def _fetch_file(fname, ftp, ftype):
    try:
        ftp.retrbinary("RETR {}".format(fname), open(fname, "wb").write)
    except error_perm:
        raise Exception("File {} not available".format(fname))
    if ftype == "DBC":
        df = read_dbc(fname, encoding="iso-8859-1")
    elif ftype == "DBF":
        dbf = DBF(fname, encoding="iso-8859-1")
        df = pd.DataFrame(list(dbf))
    os.unlink(fname)
    return df
