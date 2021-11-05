"""
Download data from the national immunization program
"""
import os
from ftplib import FTP, error_perm
from io import StringIO

import pandas as pd
from dbfread import DBF

from pysus.online_data import CACHEPATH


def download(state, year, cache=True):
    """
    Download imunization records for a given State and year.
    :param state: uf two letter code
    :param year: year in 4 digits
    :param cache: If True reads from cache if available
    :return: Dataframe
    """
    # if year < 2000:
    #     raise ValueError("PNI does not contain data before 2000")
    year2 = str(year)[-2:].zfill(2)
    state = state.upper()
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    ftp.cwd("/dissemin/publicos/PNI/DADOS")
    fname = f"CPNI{state}{year2}.DBF"

    cachefile = os.path.join(CACHEPATH, "PNI_" + fname.split(".")[0] + "_.parquet")
    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df

    try:
        ftp.retrbinary("RETR {}".format(fname), open(fname, "wb").write)
    except error_perm:
        try:
            ftp.retrbinary("RETR {}".format(fname.upper()), open(fname, "wb").write)
        except Exception as e:
            raise Exception("{}\nFile {} not available".format(e, fname))
    dbf = DBF(fname, encoding="iso-8859-1")
    df = pd.DataFrame(list(dbf))
    if cache:
        df.to_parquet(cachefile)
    os.unlink(fname)
    return df


def get_available_years(state):
    """
    Fetch available years (dbf names) for the `state`.
    :param state: uf code
    :return: list of strings (filenames)
    """
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    ftp.cwd("/dissemin/publicos/PNI/DADOS")
    # res = StringIO()
    res = ftp.nlst(f"CPNI{state}*.DBF")
    return res


def available_docs():
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    ftp.cwd("/dissemin/publicos/PNI/DOCS")
    # res = StringIO()
    res = ftp.nlst(f"*")
    return res


def fetch_document(fname):
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    ftp.cwd("/dissemin/publicos/PNI/DOCS")
    try:
        ftp.retrbinary("RETR {}".format(fname), open(fname, "wb").write)
        print(f"Downloaded {fname}.")
    except Exception as e:
        raise Exception(f"{e}\nFile {fname} not available.")
