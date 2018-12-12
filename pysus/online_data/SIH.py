u"""
Downloads SIH data from Datasus FTP server
Created on 21/09/18
by fccoelho
license: GPL V3 or Later
"""

import os
from ftplib import FTP
from pysus.utilities.readdbc import read_dbc
from pysus.online_data import CACHEPATH
from dbfread import DBF
import pandas as pd


def download(state: str, year: int, month: int, cache: bool=True) -> object:
    """
    Download SIH records for state year and month and returns dataframe
    :param month: 1 to 12
    :param state: 2 letter state code
    :param year: 4 digit integer 
    """
    state = state.upper()
    year2 = int(str(year)[-2:])
    month = str(month).zfill(2)
    if year < 1992:
        raise ValueError("SIH does not contain data before 1994")
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    if year < 2008:
        ftype = 'DBC'
        ftp.cwd('/dissemin/publicos/SIHSUS/199201_200712/Dados')
        fname = 'RD{}{}{}.dbc'.format(state, year2, month)
    if year >= 2008:
        ftype = 'DBF'
        ftp.cwd('/dissemin/publicos/SIHSUS/DBF'.format(year))
        fname = 'RD{}{}{}.dbf'.format(state, str(year2).zfill(2), month)
    cachefile = os.path.join(CACHEPATH, 'SIH_' + fname.split('.')[0] + '_.parquet')
    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df
    df = _fetch_file(fname, ftp, ftype)
    if cache:
        df.to_parquet(cachefile)
    return df


def _fetch_file(fname, ftp, ftype):
    try:
        ftp.retrbinary('RETR {}'.format(fname), open(fname, 'wb').write)
    except:
        raise Exception("File {} not available".format(fname))
    if ftype == 'DBC':
        df = read_dbc(fname, encoding='iso-8859-1')
    elif ftype == 'DBF':
        dbf = DBF(fname, encoding='iso-8859-1')
        df = pd.DataFrame(list(dbf))
    os.unlink(fname)
    return df


