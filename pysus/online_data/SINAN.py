import os
from ftplib import FTP
from pysus.utilities.readdbc import read_dbc
from pysus.online_data import CACHEPATH
from dbfread import DBF
from io import StringIO
import pandas as pd

agravos = {
    'Animais Peçonhentos': 'ANIM',
    'Botulismo': 'BOTU',
    'Chagas': 'CHAG',
    'Colera': 'COLE',
    'Coqueluche': 'COQU',
    'Dengue': 'DENG',
    'Difteria': 'DIFT',
    'Esquistossomose': 'ESQU',
    'Febre Amarela': 'FAMA',
    'Febre Maculosa': 'FMAC',
    'Febre Tifoide': 'FTIF',
    'Hanseniase': 'HANS',
    'Hantavirose': 'HANT',
    'Hepatites Virais': 'HEPA',
    'Intoxicação Exógena': 'IEXO',
    'Leishmaniose Visceral': 'LEIV',
    'Leptospirose': 'LEPT',
    'Leishmaniose Tegumentar': 'LTAN',
    'Malaria': 'MALA',
    'Meningite': 'MENI',
    'Peste': 'PEST',
    'Poliomielite': 'PFAN',
    'Raiva Humana': 'RAIV',
    'Tétano Acidental': 'TETA',
    'Tétano Neonatal': 'TETN',
    'Tuberculose': 'TUBE',
    'Violência Domestica': 'VIOL'
}


def list_diseases():
    """List available diseases on SINAN"""
    return list(agravos.keys())

def get_available_years(state, disease):
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    ftp.cwd("/dissemin/publicos/SINAN/DADOS/FINAIS")
    # res = StringIO()
    res = ftp.nlst(f'{agravos[disease.title()]}{state}*.dbc')
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
        print(f'Disease {disease} is not available in SINAN.\nAvailable diseases: {list_diseases()}')
    year2 = str(year)[-2:].zfill(2)
    state = state.upper()
    if year < 2007:
        raise ValueError("SINAN does not contain data before 2007")
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    ftp.cwd("/dissemin/publicos/SINAN/DADOS/FINAIS")
    dis_code = agravos[disease.title()]
    fname = f'{dis_code}{state}{year2}.DBC'

    cachefile = os.path.join(CACHEPATH, 'SINAN_' + fname.split('.')[0] + '_.parquet')
    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df

    try:
        ftp.retrbinary('RETR {}'.format(fname), open(fname, 'wb').write)
    except:
        try:
            ftp.retrbinary('RETR {}'.format(fname.upper()), open(fname, 'wb').write)
        except Exception as e:
            raise Exception("{}\nFile {} not available".format(e, fname))

    df = read_dbc(fname, encoding='iso-8859-1')
    if cache:
        df.to_parquet(cachefile)
    os.unlink(fname)
    return df
