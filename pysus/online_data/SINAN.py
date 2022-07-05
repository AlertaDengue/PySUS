import os
from ftplib import FTP, error_perm
from io import StringIO
import warnings

# import download as download
import pandas as pd
from dbfread import DBF

from pysus.online_data import CACHEPATH, _fetch_file
from pysus.utilities.readdbc import read_dbc

agravos = {
    "Animais Peçonhentos": "ANIM",
    "Botulismo": "BOTU",
    "Cancer": "CANC",
    "Chagas": "CHAG",
    "Chikungunya": "CHIK",
    "Colera": "COLE",
    "Coqueluche": "COQU",
    "Contact Communicable Disease": "ACBI",
    "Acidentes de Trabalho": "ACGR",
    "Dengue": "DENG",
    "Difteria": "DIFT",
    "Esquistossomose": "ESQU",
    "Febre Amarela": "FAMA",
    "Febre Maculosa": "FMAC",
    "Febre Tifoide": "FTIF",
    "Hanseniase": "HANS",
    "Hantavirose": "HANT",
    "Hepatites Virais": "HEPA",
    "Intoxicação Exógena": "IEXO",
    "Leishmaniose Visceral": "LEIV",
    "Leptospirose": "LEPT",
    "Leishmaniose Tegumentar": "LTAN",
    "Malaria": "MALA",
    "Meningite": "MENI",
    "Peste": "PEST",
    "Poliomielite": "PFAN",
    "Raiva Humana": "RAIV",
    "Sífilis Adquirida": "SIFA",
    "Sífilis Congênita": "SIFC",
    "Sífilis em Gestante": "SIFG",
    "Tétano Acidental": "TETA",
    "Tétano Neonatal": "TETN",
    "Tuberculose": "TUBE",
    "Violência Domestica": "VIOL",
    "Zika": "ZIKA"
}


def list_diseases():
    """List available diseases on SINAN"""
    return list(agravos.keys())


def get_available_years(state, disease):
    """
    Fetch available years for data related to specific disease and state
    :param state: Two letter state symbol, e.g. 'RJ', 'BR' is also possible for national level.
    :param disease: Disease name. See `SINAN.list_diseases` for valid names
    """
    warnings.warn("Now SINAN tables are no longer split by state. Returning countrywide years")
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    ftp.cwd("/dissemin/publicos/SINAN/DADOS/FINAIS")
    # res = StringIO()
    res = ftp.nlst(f"{agravos[disease.title()]}BR*.dbc")
    return res

def download(state, year, disease, cache=True):
    """
    Downloads SINAN data directly from Datasus ftp server
    :param state: two-letter state identifier: MG == Minas Gerais
    :param year: 4 digit integer
    :disease: Diseases
    :return: pandas dataframe
    """
    try:
        assert disease.title() in agravos
    except AssertionError:
        print(
            f"Disease {disease} is not available in SINAN.\nAvailable diseases: {list_diseases()}"
        )
    year2 = str(year)[-2:].zfill(2)
    state = 'BR' # state.upper()
    warnings.warn("Now SINAN tables are no longer split by state. Returning country table")
    if year < 2007:
        raise ValueError("SINAN does not contain data before 2007")
    
    dis_code = agravos[disease.title()]
    fname = f"{dis_code}{state}{year2}.DBC"
    path = "/dissemin/publicos/SINAN/DADOS/FINAIS"
    path_pre = "/dissemin/publicos/SINAN/DADOS/PRELIM"
    cachefile = os.path.join(CACHEPATH, "SINAN_" + fname.split(".")[0] + "_.parquet")

    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    ftp.cwd(path)
    try:
        df = _fetch_file(fname, path, 'DBC')
    except:  # If file is not part of the final releases
        df = _fetch_file(fname, path_pre, 'DBC')

    if cache:
        df.to_parquet(cachefile)
    if os.path.exists(fname):
        os.unlink(fname)
    return df
