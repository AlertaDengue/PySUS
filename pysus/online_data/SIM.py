"""
Download Mortality records from SIM Datasus
Created on 12/12/18
by fccoelho
license: GPL V3 or Later
"""
import os
from ftplib import FTP, error_perm
from typing import Union

import pandas as pd
from dbfread import DBF
from loguru import logger
from pysus.online_data import CACHEPATH, FTP_Downloader


def download(
    states: Union[str, list],
    years: Union[str, list, int],
    data_dir: str = CACHEPATH,
):
    """
    Downloads data directly from Datasus ftp server
    :param states: two-letter state identifier: MG == Minas Gerais
                   can be a list
    :param years: 4 digit integer, can be a list
    :return: a list of downloaded parquet paths
    """
    return FTP_Downloader("SIM").download(
        UFs=states, years=years, local_dir=data_dir
    )


def get_CID10_chapters_table(cache=True):
    """
    Fetch the CID10 chapters table
    :param cache: If set to True, stores data as parquets.
    :return: Pandas DataFrame
    """
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    logger.debug(
        f"Stablishing connection with ftp.datasus.gov.br.\n{ftp.welcome}"
    )
    ftp.cwd("/dissemin/publicos/SIM/CID10/TABELAS")
    logger.debug(
        "Changing FTP work dir to: /dissemin/publicos/SIM/CID10/TABELAS"
    )

    fname = "CIDCAP10.DBF"
    cachefile = os.path.join(
        CACHEPATH, "SIM_" + fname.split(".")[0] + "_.parquet"
    )

    if os.path.exists(cachefile):
        logger.info(f"Local parquet file found at {cachefile}")
        df = pd.read_parquet(cachefile)

        return df

    try:
        ftp.retrbinary("RETR {}".format(fname), open(fname, "wb").write)

    except error_perm:
        raise Exception("Could not download {}".format(fname))

    dbf = DBF(fname, encoding="iso-8859-1")
    df = pd.DataFrame(list(dbf))

    if cache:
        df.to_parquet(cachefile)
        logger.info(f"Data stored as parquet at {cachefile}")

    os.unlink(fname)
    logger.debug(f"{fname} removed")

    return df


def get_CID10_table(cache=True):
    """
    Fetch the CID10 table
    :param cache: If set to True, stores data as parquets.
    :return: Pandas DataFrame
    """
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    logger.debug(
        f"Stablishing connection with ftp.datasus.gov.br.\n{ftp.welcome}"
    )
    ftp.cwd("/dissemin/publicos/SIM/CID10/TABELAS")
    logger.debug(
        "Changing FTP work dir to: /dissemin/publicos/SIM/CID10/TABELAS"
    )

    fname = "CID10.DBF"
    cachefile = os.path.join(
        CACHEPATH, "SIM_" + fname.split(".")[0] + "_.parquet"
    )

    if os.path.exists(cachefile):
        logger.info(f"Local parquet file found at {cachefile}")
        df = pd.read_parquet(cachefile)

        return df

    try:
        ftp.retrbinary("RETR {}".format(fname), open(fname, "wb").write)

    except error_perm:
        raise Exception("Could not download {}".format(fname))

    dbf = DBF(fname, encoding="iso-8859-1")
    df = pd.DataFrame(list(dbf))

    if cache:
        df.to_parquet(cachefile)
        logger.info(f"Data stored as parquet at {cachefile}")

    os.unlink(fname)
    logger.debug(f"{fname} removed")

    return df


def get_CID9_table(cache=True):
    """
    Fetch the CID9 table
    :param cache: If set to True, stores data as parquets.
    :return: Pandas DataFrame
    """
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    logger.debug(
        f"Stablishing connection with ftp.datasus.gov.br.\n{ftp.welcome}"
    )
    ftp.cwd("/dissemin/publicos/SIM/CID9/TABELAS")
    logger.debug(
        "Changing FTP work dir to: /dissemin/publicos/SIM/CID9/TABELAS"
    )

    fname = "CID9.DBF"
    cachefile = os.path.join(
        CACHEPATH, "SIM_" + fname.split(".")[0] + "_.parquet"
    )

    if os.path.exists(cachefile):
        logger.info(f"Local parquet file found at {cachefile}")
        df = pd.read_parquet(cachefile)

        return df

    try:
        ftp.retrbinary("RETR {}".format(fname), open(fname, "wb").write)

    except error_perm:
        raise Exception("Could not download {}".format(fname))

    dbf = DBF(fname, encoding="iso-8859-1")
    df = pd.DataFrame(list(dbf))

    if cache:
        df.to_parquet(cachefile)
        logger.info(f"Data stored as parquet at {cachefile}")

    os.unlink(fname)
    logger.debug(f"{fname} removed")

    return df


def get_municipios(cache=True):
    """
    Get municipality metadata
    :param cache: If set to True, stores data as parquets.
    :return: Pandas DataFrame
    """
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    logger.debug(
        f"Stablishing connection with ftp.datasus.gov.br.\n{ftp.welcome}"
    )
    ftp.cwd("/dissemin/publicos/SIM/CID10/TABELAS")
    logger.debug(
        "Changing FTP work dir to: /dissemin/publicos/SIM/CID10/TABELAS"
    )

    fname = "CADMUN.DBF"
    cachefile = os.path.join(
        CACHEPATH, "SIM_" + fname.split(".")[0] + "_.parquet"
    )

    if os.path.exists(cachefile):
        logger.info(f"Local parquet file found at {cachefile}")
        df = pd.read_parquet(cachefile)

        return df

    try:
        ftp.retrbinary("RETR {}".format(fname), open(fname, "wb").write)

    except:
        raise Exception("Could not download {}".format(fname))

    dbf = DBF(fname, encoding="iso-8859-1")
    df = pd.DataFrame(list(dbf))

    if cache:
        df.to_parquet(cachefile)
        logger.info(f"Data stored as parquet at {cachefile}")

    os.unlink(fname)
    logger.debug(f"{fname} removed")

    return df


def get_ocupations(cache=True):
    """
    Fetch ocupations table
    :param cache: If set to True, stores data as parquets.
    :return: Pandas DataFrame
    """
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    logger.debug(
        f"Stablishing connection with ftp.datasus.gov.br.\n{ftp.welcome}"
    )
    ftp.cwd("/dissemin/publicos/SIM/CID10/TABELAS")
    logger.debug(
        "Changing FTP work dir to: /dissemin/publicos/SIM/CID10/TABELAS"
    )
    fname = "TABOCUP.DBF"
    cachefile = os.path.join(
        CACHEPATH, "SIM_" + fname.split(".")[0] + "_.parquet"
    )

    if os.path.exists(cachefile):
        logger.info(f"Local parquet file found at {cachefile}")
        df = pd.read_parquet(cachefile)

        return df

    try:
        ftp.retrbinary("RETR {}".format(fname), open(fname, "wb").write)

    except:
        raise Exception("Could not download {}".format(fname))

    dbf = DBF(fname, encoding="iso-8859-1")
    df = pd.DataFrame(list(dbf))

    if cache:
        df.to_parquet(cachefile)
        logger.info(f"Data stored as parquet at {cachefile}")

    os.unlink(fname)
    logger.debug(f"{fname} removed")

    return df
