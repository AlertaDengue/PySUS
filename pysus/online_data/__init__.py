u"""
Created on 21/09/18
by fccoelho
license: GPL V3 or Later
"""
import os
from ftplib import FTP
from pathlib import Path

import pandas as pd
from dbfread import DBF

from pysus.utilities.readdbc import read_dbc, dbc2dbf

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


def _fetch_file(fname: str, path: str, ftype: str, return_df: bool=True) -> pd.DataFrame:
    """
    Fetch a single file.
    :param fname: Name of the file
    :param path: ftp path where file is located
    :param ftype: 'DBC' or 'DBF'
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
    if return_df:
        df = get_dataframe(fname, ftype)
        return df
    else:
        return pd.DataFrame()


def get_dataframe(fname: str, ftype: str) -> pd.DataFrame:
    """
    Return a dataframe read fom temporary file on disk.
    :param fname: temporary file name
    :param ftype: 'DBC' or 'DBF'
    :return:  DataFrame
    """
    if ftype == "DBC":
        df = read_dbc(fname, encoding="iso-8859-1")
    elif ftype == "DBF":
        dbf = DBF(fname, encoding="iso-8859-1")
        df = pd.DataFrame(list(dbf))
    if os.path.exists(fname):
        os.unlink(fname)
    return df

def get_chunked_dataframe(fname: str, ftype: str) -> str:
    if ftype == "DBC":
        outname = fname.replace('DBC', 'DBF')
        dbc2dbf(fname, outname)

    tempfile = outname.replace('DBF', 'csv.gz')
    first = 1
    for d in stream_DBF(DBF(outname, encoding="iso-8859-1")):
        df = pd.DataFrame(d)
        if first:
            df.to_csv(tempfile)
            first = 0
        else:
            df.to_csv(tempfile, mode='a', header=False)

    if os.path.exists(fname):
        os.unlink(fname)
        os.unlink(outname)

    return tempfile


def stream_DBF(dbf, chunk_size=30000):
    """Fetches records in chunks to preserve memory"""
    data = []
    i = 0
    for records in dbf:
        data.append(records)
        i += 1
        if i == chunk_size:
            yield data
            data = []
            i = 0
    else:
        return data

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
