import logging
import os
import warnings
from ftplib import FTP
from pathlib import Path

from pysus.online_data import (
    CACHEPATH,
    _fetch_file,
    get_chunked_dataframe,
    get_dataframe,
)
from pysus.utilities.readdbc import dbc2dbf

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
    "Zika": "ZIKA",
}


def list_diseases():
    """List available diseases on SINAN"""
    return list(agravos.keys())


def get_available_years(disease, return_path=False):
    """
    Fetch available years for data related to specific disease and state
    :param state: Two letter state symbol, e.g. 'RJ', 'BR' is also possible for national level.
    :param disease: Disease name. See `SINAN.list_diseases` for valid names
    """
    warnings.warn(
        "Now SINAN tables are no longer split by state. Returning countrywide years"
    )
    disease = check_case(disease)
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    ftp.cwd("/dissemin/publicos/SINAN/DADOS/FINAIS")

    if not ftp.nlst(f"{agravos[disease]}BR*.dbc"):
        ftp.cwd("/dissemin/publicos/SINAN/DADOS/PRELIM")

        if return_path:
            ppath = "/dissemin/publicos/SINAN/DADOS/PRELIM"
            dbcs = []
            for dbc in ftp.nlst(f"{agravos[disease]}BR*.dbc"):
                dbcs.append(f"{ppath}/{dbc}")
                return dbcs

    if return_path:
        ppath = "/dissemin/publicos/SINAN/DADOS/FINAIS"
        dbcs = []
        for dbc in ftp.nlst(f"{agravos[disease]}BR*.dbc"):
            dbcs.append(f"{ppath}/{dbc}")
        return dbcs

    return ftp.nlst(f"{agravos[disease]}BR*.dbc")


def download(year, disease, cache=True, return_fname=False):
    """
    Downloads SINAN data directly from Datasus ftp server
    :param state: two-letter state identifier: MG == Minas Gerais
    :param year: 4 digit integer
    :disease: Diseases
    :return: pandas dataframe
    """
    disease = check_case(disease)
    year2 = str(year)[-2:].zfill(2)
    if not get_available_years(disease):
        raise Exception(f"No data is available at present for {disease}")
    first_year = [f.split(".")[0][-2:] for f in get_available_years(disease)][
        0
    ]
    state = "BR"  # state.upper()
    warnings.warn(
        "Now SINAN tables are no longer split by state. Returning country table"
    )
    if year2 < first_year:
        raise ValueError(f"SINAN does not contain data before {first_year}")

    dis_code = agravos[disease]
    fname = f"{dis_code}{state}{year2}.DBC"
    path = "/dissemin/publicos/SINAN/DADOS/FINAIS"
    path_pre = "/dissemin/publicos/SINAN/DADOS/PRELIM"
    cachefile = os.path.join(
        CACHEPATH, "SINAN_" + fname.split(".")[0] + "_.parquet"
    )

    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    ftp.cwd(path)
    try:
        _fetch_file(fname, path, "DBC", return_df=False)
    except:  # If file is not part of the final releases
        _fetch_file(fname, path_pre, "DBC", return_df=False)
    if return_fname:
        filename = get_chunked_dataframe(fname, "DBC")
        return filename
    else:
        df = get_dataframe(fname, "DBC")
    if cache:
        df.to_parquet(cachefile)
    if os.path.exists(fname):
        os.unlink(fname)
    return df


def download_dbfs(disease, data_dir="/tmp/pysus"):
    """
    Download all DBFs found in datasus, given a disease.
    An output path can be defined.
    :param disease: A disease according to `agravos`.
    :param data_dir: Out
    """
    disease = check_case(disease)

    for dbc in get_available_years(disease, return_path=True):
        if any(get_available_years(disease, return_path=True)):
            fname = dbc.split("/")[-1]
            path = dbc.split("/")[:-1]
            path = "/".join(path)
            outname = fname.replace(fname[-3:], "dbf")
            pathout = Path(data_dir)

            if not Path(pathout / outname).exists():
                pathout.mkdir(parents=True, exist_ok=True)

                try:
                    _fetch_file(fname, path, "DBC", return_df=False)

                except Exception as e:
                    logging.error(f"Not able to fetch {fname}\n\n{e}")

                if Path(fname).exists():

                    try:
                        dbc2dbf(fname, str(Path(pathout / outname)))
                        logging.info(f"{outname} downloaded at {pathout}")

                    except Exception as e:
                        logging.error(
                            f"Not able to parse {fname} to DBF\n\n{e}"
                        )

                    finally:
                        Path(fname).unlink()


def check_case(disease):
    try:
        assert disease in agravos
    except AssertionError:
        try:
            assert disease.title()
            disease = disease.title()
        except AssertionError:
            print(
                f"Disease {disease.title()} is not available in SINAN.\nAvailable diseases: {list_diseases()}"
            )
    return disease
