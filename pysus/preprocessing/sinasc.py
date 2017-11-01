"""
Download SINASC data from DATASUS FTP server
Created on 01/11/17
by fccoelho
license: GPL V3 or Later
"""
import os
from ftplib import FTP
from pysus.utilities.readdbc import read_dbc


def download(state, year):
    """
    Downloads data directly from Datasus ftp server
    :param state: two-letter state identifier: MG == Minas Gerais
    :param year: 4 digit integer
    :return: pandas dataframe
    """
    if year < 1994:
        raise ValueError("SINASC does not contain data before 1994")
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    if year >= 1996:
        ftp.cwd('/dissemin/publicos/SINASC/NOV/DNRES')
        fname = 'DN{}{}.DBC'.format(state, year)
    else:
        ftp.cwd('/dissemin/publicos/SINASC/ANT/DNRES')
        fname = 'DNR{}{}.DBC'.format(state, str(year)[-2:])


    ftp.retrbinary('RETR {}'.format(fname), open(fname, 'wb').write)
    df = read_dbc(fname, encoding='iso-8859-1')
    os.unlink(fname)
    return df


