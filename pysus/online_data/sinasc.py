"""
Download SINASC data from DATASUS FTP server
Created on 01/11/17
by fccoelho
license: GPL V3 or Later
"""
import os
import warnings
import pandas as pd

from ftplib import FTP
from loguru import logger

from pysus.online_data import CACHEPATH
from pysus.utilities.readdbc import read_dbc

warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")


def download(state, year, cache=True):
    """
    Downloads data directly from Datasus ftp server
    :param state: two-letter state identifier: MG == Minas Gerais
    :param year: 4 digit integer
    :return: pandas dataframe
    """
    assert len(str(year)) == 4
    state = state.upper()

    if year < 1994:
        raise ValueError("SINASC does not contain data before 1994")

    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    logger.debug(f"Stablishing connection with ftp.datasus.gov.br.\n{ftp.welcome}")

    if year >= 1996:
        ftp.cwd("/dissemin/publicos/SINASC/NOV/DNRES")
        logger.debug("Changing FTP work dir to: /dissemin/publicos/SINASC/NOV/DNRES")
        fname = "DN{}{}.DBC".format(state, year)

    else:
        ftp.cwd("/dissemin/publicos/SINASC/ANT/DNRES")
        logger.debug("Changing FTP work dir to: /dissemin/publicos/SINASC/ANT/DNRES")
        fname = "DNR{}{}.DBC".format(state, str(year)[-2:])

    cachefile = os.path.join(CACHEPATH, "SINASC_" + fname.split(".")[0] + "_.parquet")

    if os.path.exists(cachefile):
        logger.info(f"Local parquet file found at {cachefile}")
        df = pd.read_parquet(cachefile)

        return df

    ftp.retrbinary("RETR {}".format(fname), open(fname, "wb").write)
    df = read_dbc(fname, encoding="iso-8859-1")

    if cache:
        df.to_parquet(cachefile)
        logger.info(f"Data stored as parquet at {cachefile}")

    os.unlink(fname)
    logger.debug(f"{fname} removed")

    return df


def get_available_years(state):
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    logger.debug(f"Stablishing connection with ftp.datasus.gov.br.\n{ftp.welcome}")

    ftp.cwd("/dissemin/publicos/SINASC/ANT/DNRES")
    logger.debug("Changing FTP work dir to: /dissemin/publicos/SINASC/ANT/DNRES")
    res = ftp.nlst(f"DNR{state}*.*")

    ftp.cwd("/dissemin/publicos/SINASC/NOV/DNRES")
    logger.debug("Changing FTP work dir to: /dissemin/publicos/SINASC/NOV/DNRES")
    res += ftp.nlst(f"DN{state}*.*")
    
    return res
