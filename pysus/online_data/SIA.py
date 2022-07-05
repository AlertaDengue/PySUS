"""
Downloads SIA data from Datasus FTP server
Created on 21/09/18
by fccoelho
Modified on 18/04/21
by bcbernardo
license: GPL V3 or Later
"""

import os
import warnings
from datetime import date
from ftplib import FTP
from typing import Dict, List, Optional, Tuple, Union
from pprint import pprint

import pandas as pd
from dbfread import DBF

from pysus.online_data import CACHEPATH
from pysus.utilities.readdbc import read_dbc, read_dbc_dbf, dbc2dbf

group_dict: Dict[str, Tuple[str, int, int]] = {
    "PA": ("Produção Ambulatorial", 7, 1994),
    "BI": ("Boletim de Produção Ambulatorial individualizado", 1, 2008),
    "AD": ("APAC de Laudos Diversos", 1, 2008),
    "AM": ("APAC de Medicamentos", 1, 2008),
    "AN": ("APAC de Nefrologia", 1, 2008),
    "AQ": ("APAC de Quimioterapia", 1, 2008),
    "AR": ("APAC de Radioterapia", 1, 2008),
    "AB": ("APAC de Cirurgia Bariátrica", 1, 2008),
    "ACF": ("APAC de Confecção de Fístula", 1, 2008),
    "ATD": ("APAC de Tratamento Dialítico", 1, 2008),
    "AMP": ("APAC de Acompanhamento Multiprofissional", 1, 2008),
    "SAD": ("RAAS de Atenção Domiciliar", 1, 2008),
    "PS": ("RAAS Psicossocial", 1, 2008),
}

def show_datatypes():
    pprint(group_dict)

def download(
        state: str,
        year: int,
        month: int,
        cache: bool = True,
        group: Union[str, List[str]] = ["PA", "BI"],
) -> Union[Optional[pd.DataFrame], Tuple[Optional[pd.DataFrame], ...]]:
    """
    Download SIASUS records for state year and month and returns dataframe
    :param month: 1 to 12
    :param state: 2 letter state code
    :param year: 4 digit integer
    :param cache: whether to cache files locally. default is True
    :param groups: 2-3 letter document code or a list of 2-3 letter codes,
        defaults to ['PA', 'BI']. Codes should be one of the following:
        PA - Produção Ambulatorial
        BI - Boletim de Produção Ambulatorial individualizado
        AD - APAC de Laudos Diversos
        AM - APAC de Medicamentos
        AN - APAC de Nefrologia
        AQ - APAC de Quimioterapia
        AR - APAC de Radioterapia
        AB - APAC de Cirurgia Bariátrica
        ACF - APAC de Confecção de Fístula
        ATD - APAC de Tratamento Dialítico
        AMP - APAC de Acompanhamento Multiprofissional
        SAD - RAAS de Atenção Domiciliar
        PS - RAAS Psicossocial
    :return: A tuple of dataframes with the documents in the order given
        by the , when they are found
    """
    state = state.upper()
    year2 = str(year)[-2:]
    month = str(month).zfill(2)
    if isinstance(group, str):
        group = [group]
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    ftype = "DBC"
    if year >= 1994 and year < 2008:
        ftp.cwd("/dissemin/publicos/SIASUS/199407_200712/Dados")
    elif year >= 2008:
        ftp.cwd("/dissemin/publicos/SIASUS/200801_/Dados")
    else:
        raise ValueError("SIA does not contain data before 1994")

    dfs: List[Optional[pd.DataFrame]] = []
    for gname in group:
        gname = gname.upper()
        if gname not in group_dict:
            raise ValueError(f"SIA does not contain files named {gname}")

        # Check available
        input_date = date(int(year), int(month), 1)
        available_date = date(group_dict[gname][2], group_dict[gname][1], 1)
        if input_date < available_date:
            dfs.append(None)
            # NOTE: raise Warning instead of ValueError for
            # backwards-compatibility with older behavior of returning
            # (PA, None) for calls after 1994 and before Jan, 2008
            warnings.warn(
                f"SIA does not contain data for {gname} "
                f"before {available_date:%d/%m/%Y}"
            )
            continue

        fname = f"{gname}{state}{year2.zfill(2)}{month}.dbc"

        # Check in Cache
        cachefile = os.path.join(CACHEPATH, "SIA_" + fname.split(".")[0] + "_.parquet")
        if os.path.exists(cachefile):
            df = pd.read_parquet(cachefile)
        else:
            try:
                df = _fetch_file(fname, ftp, ftype)
                if cache and df:  # saves to cache if df is not None
                    df.to_parquet(cachefile)
            except Exception as e:
                df = None
                print(e)

        dfs.append(df)

    return tuple(dfs)


def _fetch_file(fname, ftp, ftype):
    """
    Does the FTP fetching.
    :param fname: file name
    :param ftp: ftp connection object
    :param ftype: file type: DBF|DBC
    :return: pandas dataframe
    """

    multiples = False
    fnames = check_file_split(fname, ftp)

    multiples = len(fnames) > 1

    if multiples:
        download_multiples(fnames, ftp)
        print(f"This download is split into the following files: {fnames}\n"
              f"They have been downloaded in {CACHEPATH}.\n"
              f"To load them, use the pysus.utilities.read_dbc_dbf function.")
        return
    df = read_dbc_dbf(fname)

    os.unlink(fname)
    return df


def download_multiples(fnames, ftp):
    for fn in fnames:
        fnfull = os.path.join(CACHEPATH, fn)
        print(f"Downloading {fn}...")
        fobj = open(fnfull, "wb")
        try:
            ftp.retrbinary(f"RETR {fn}", fobj.write)
            dbc2dbf(fnfull, fnfull.replace('.dbc', '.dbf'))
            os.unlink(fnfull)
        except Exception as exc:
            raise Exception(f"Retrieval of file {fn} failed with the following error:\n {exc}")


def check_file_split(fname: str, ftp: FTP) -> list:
    """
    Check for split filenames. Sometimes when files are too large, they are split into multiple files ending in a, b, c, ...
    :param fname: filename
    :param ftp: ftp conection
    :return: list
    """
    files = []
    flist = ftp.nlst()
    if fname not in flist:
        for l in ['a', 'b', 'c', 'd']:
            nm, ext = fname.split('.')
            if f'{nm}{l}.{ext}' in flist:
                files.append(f'{nm}{l}.{ext}')

    return files
