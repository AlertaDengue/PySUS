u"""
Download Mortality records from SIM Datasus
Created on 12/12/18
by fccoelho
license: GPL V3 or Later
"""

import os
from ftplib import FTP
from pysus.utilities.readdbc import read_dbc
from pysus.online_data import CACHEPATH
from dbfread import DBF
import pandas as pd


def download(state, year, cache=True, folder=None):
    """
    Downloads data directly from Datasus ftp server
    :param state: two-letter state identifier: MG == Minas Gerais
    :param year: 4 digit integer
    :return: pandas dataframe
    """
    year2 = str(year)[-2:].zfill(2)
    state = state.upper()
    ftp_dir = ""
    fname = ""
    if year < 1979:
        raise ValueError("SIM does not contain data before 1979")
    elif year >= 1996:
        ftp_dir = '/dissemin/publicos/SIM/CID10/DORES'
        fname = 'DO{}{}.DBC'.format(state, year)
    else:
        ftp_dir = '/dissemin/publicos/SIM/CID9/DORES'
        fname = fname = 'DOR{}{}.DBC'.format(state, year2)
    


    cache_fail = False
    cachefile = os.path.join(CACHEPATH, 'SIM_'+fname.split('.')[0] + '_.parquet')
    if folder:
        fname = "{}/{}".format(folder,fname)
    elif cache:
        if os.path.exists(cachefile):
            df = pd.read_parquet(cachefile)
            return df
        else:
            cache_fail = True

    # Se tiver folder não tenta cache
    if not folder and (cache_fail or not cache):
        ftp = FTP('ftp.datasus.gov.br')
        ftp.login()
        ftp.cwd(ftp_dir)
    
        try:
            ftp.retrbinary('RETR {}'.format(fname), open(fname, 'wb').write)
        except:
            try:
                ftp.retrbinary('RETR {}'.format(fname.upper()), open(fname, 'wb').write)
            except:
                raise Exception("File {} not available".format(fname))

    df = read_dbc(fname, encoding='iso-8859-1')
    
    df.to_parquet(cachefile)
            
    os.unlink(fname)
    return df


def get_CID10_table(cache=True):
    """
    Fetch the CID10 table
    :param cache:
    :return:
    """
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    ftp.cwd('/dissemin/publicos/SIM/CID10/TABELAS')
    fname = 'CID10.DBF'
    cachefile = os.path.join(CACHEPATH, 'SIM_' + fname.split('.')[0] + '_.parquet')
    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df
    try:
        ftp.retrbinary('RETR {}'.format(fname), open(fname, 'wb').write)
    except:
        raise Exception('Could not download {}'.format(fname))
    dbf = DBF(fname, encoding='iso-8859-1')
    df = pd.DataFrame(list(dbf))
    if cache:
        df.to_parquet(cachefile)
    os.unlink(fname)
    return df


def get_CID9_table(cache=True):
    """
    Fetch the CID9 table
    :param cache:
    :return:
    """
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    ftp.cwd('/dissemin/publicos/SIM/CID9/TABELAS')
    fname = 'CID9.DBF'
    cachefile = os.path.join(CACHEPATH, 'SIM_' + fname.split('.')[0] + '_.parquet')
    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df
    try:
        ftp.retrbinary('RETR {}'.format(fname), open(fname, 'wb').write)
    except:
        raise Exception('Could not download {}'.format(fname))
    dbf = DBF(fname, encoding='iso-8859-1')
    df = pd.DataFrame(list(dbf))
    if cache:
        df.to_parquet(cachefile)
    os.unlink(fname)
    return df


def get_municipios(cache=True):
    """
    Get municipality metadata
    :param cache:
    :return:
    """
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    ftp.cwd('/dissemin/publicos/SIM/CID10/TABELAS')
    fname = 'CADMUN.DBF'
    cachefile = os.path.join(CACHEPATH, 'SIM_' + fname.split('.')[0] + '_.parquet')
    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df
    try:
        ftp.retrbinary('RETR {}'.format(fname), open(fname, 'wb').write)
    except:
        raise Exception('Could not download {}'.format(fname))
    dbf = DBF(fname, encoding='iso-8859-1')
    df = pd.DataFrame(list(dbf))
    if cache:
        df.to_parquet(cachefile)
    os.unlink(fname)
    return df

def get_ocupations(cache=True):
    """
    Fetch ocupations table
    :param cache:
    :return:
    """
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    ftp.cwd('/dissemin/publicos/SIM/CID10/TABELAS')
    fname = 'TABOCUP.DBF'
    cachefile = os.path.join(CACHEPATH, 'SIM_' + fname.split('.')[0] + '_.parquet')
    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df
    try:
        ftp.retrbinary('RETR {}'.format(fname), open(fname, 'wb').write)
    except:
        raise Exception('Could not download {}'.format(fname))
    dbf = DBF(fname, encoding='iso-8859-1')
    df = pd.DataFrame(list(dbf))
    if cache:
        df.to_parquet(cachefile)
    os.unlink(fname)
    return df
