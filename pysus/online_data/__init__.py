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


def _fetch_file(fname, path, ftype):
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
    os.unlink(fname)
    return df
