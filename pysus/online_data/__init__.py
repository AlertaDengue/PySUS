u"""
Created on 21/09/18
by fccoelho
license: GPL V3 or Later
"""
import os
from pathlib import Path
from ftplib import FTP, error_perm
from dbfread import DBF
from pysus.utilities.readdbc import read_dbc
import pandas as pd

CACHEPATH = os.getenv("PYSUS_CACHEPATH", os.path.join(str(Path.home()), "pysus"))

# create pysus cache directory
if not os.path.exists(CACHEPATH):
    os.mkdir(CACHEPATH)


def cache_contents():
    """
    List the files currently cached in ~/pysus
    :return:
    """
    cached_data = os.listdir(CACHEPATH)
    return [os.path.join(CACHEPATH, f) for f in cached_data]


def _fetch_file(fname: str, path: str, ftype: str) -> pd.DataFrame:
    """
    Fetch a single file.
    :return:
    Pandas Dataframe
    """
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

    if os.path.exists(fname):
        os.unlink(fname)
    return df


def get_CID10_table(cache=True):
    """
    Fetch the CID10 table
    :param cache:
    :return:
    """
    fname = "CID10.DBF"
    cachefile = os.path.join(CACHEPATH, "SIM_" + fname.split(".")[0] + "_.parquet")
    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df
    df = _fetch_file(fname, "/dissemin/publicos/SIM/CID10/TABELAS", "DBF")
    if cache:
        df.to_parquet(cachefile)
    return df
