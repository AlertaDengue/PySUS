import logging
import shutil
import warnings
from ftplib import FTP
from pathlib import Path

from pysus.online_data import (
    _fetch_file,
    chunk_dbfiles_into_parquets,
    parquets_to_dataframe,
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
    ) #legacy

    fpath = "/dissemin/publicos/SINAN/DADOS/FINAIS"
    ppath = "/dissemin/publicos/SINAN/DADOS/PRELIM"
    disease = check_case(disease)

    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()

    dbcs = []

    ftp.cwd(fpath)
    for dbc in ftp.nlst(f"{agravos[disease]}BR*.dbc"):
        if return_path:
            dbcs.append(f"{fpath}/{dbc}")
        else:
            dbcs.append(dbc)
        
    ftp.cwd(ppath)
    for dbc in ftp.nlst(f"{agravos[disease]}BR*.dbc"):
        if return_path:
            dbcs.append(f"{ppath}/{dbc}")
        else:
            dbcs.append(dbc)

    return dbcs


def download(disease, year, data_path="/tmp/pysus", return_chunks=False):
    """
    Downloads SINAN data directly from Datasus ftp server
    :param year: 4 digit integer
    :disease: Diseases
    :return: pandas dataframe
    """
    disease = check_case(disease)
    year2 = str(year)[-2:].zfill(2)
    dis_code = agravos[disease]
    fname = f"{dis_code}BR{year2}.dbc"
    years = get_available_years(disease)
    fyears = get_available_years(disease, return_path=True)

    first_year = [f.split(".")[0][-2:] for f in years][
        0
    ]

    if not years or fname not in years:
        raise Exception(f"No data found for this request. Available data for {disease}: \n{years}")

    if year2 < first_year: #legacy
        raise ValueError(f"SINAN does not contain data before {first_year}")

    warnings.warn(
        "Now SINAN tables are no longer split by state. Returning country table" 
    ) #legacy
    
    pname = next(p for p in fyears if fname in p)
    sus_path = "/".join(pname.split("/")[:-1])

    data_path = Path(data_path)
    data_path.mkdir(exist_ok=True, parents=True)
    out = Path(data_path) / fname
    dbf = Path(f"{str(out)[:-4]}.dbf")

    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()

    if not Path(out).exists():
        try:
            _fetch_file(fname, sus_path, "DBC", return_df=False)
            shutil.move(Path(fname), data_path)
            logging.info(f"{fname} downloaded at {data_path}")

        except Exception as e:
            logging.error(e)

    try:
        partquet_dir = chunk_dbfiles_into_parquets(str(out))

        if not return_chunks:
            df = parquets_to_dataframe(partquet_dir, clean_after_read=True)
            return df

        return partquet_dir

    except Exception as e:
        logging.error(e)

    finally:
        while out.exists():
            out.unlink()
        while dbf.exists():
            dbf.unlink()


def download_all_years_in_chunks(disease, data_dir="/tmp/pysus"):
    """
    Download all DBFs found in datasus, given a disease, in chunks.
    An output path can be defined.
    `pysus.online_data.parquets_to_dataframe()` can read these parquets.
    :param disease: A disease according to `agravos`.
    :param data_dir: Output parquet path.
    """
    disease = check_case(disease)
    parquets = []

    for dbc in get_available_years(disease, return_path=True):
        if any(get_available_years(disease, return_path=True)):

            year = dbc.split('.dbc')[0][-2:]
            parquet_dir = download(disease, year, data_dir, return_chunks=True)
            parquets.append(parquet_dir)

    return parquets


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
