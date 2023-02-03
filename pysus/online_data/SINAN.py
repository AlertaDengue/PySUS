import shutil
from ftplib import FTP
from pathlib import Path
from loguru import logger

from pysus.online_data import (
    _fetch_file,
    chunk_dbfiles_into_parquets,
    parquets_to_dataframe,
)


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
    Fetch available years for data related to specific disease
    :param disease: Disease name. See `SINAN.list_diseases` for valid names
    :param return_path: If set to True, returns the entire Path of the datasets
                        in the FTP Server. Used to remove the discrimination of
                        FINAIS and PRELIM while downloading the datasets.
    :return: A list of DBC files from a specific disease found in the FTP Server.
    """
    logger.warning(
        "Now SINAN tables are no longer split by state. Returning countrywide years"
    ) #legacy

    fpath = "/dissemin/publicos/SINAN/DADOS/FINAIS"
    ppath = "/dissemin/publicos/SINAN/DADOS/PRELIM"
    disease = check_case(disease)

    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    logger.debug(f"Stablishing connection with ftp.datasus.gov.br.\n{ftp.welcome}")

    dbcs = []

    ftp.cwd(fpath)
    logger.debug(f"Changing FTP work dir to: {fpath}")
    for dbc in ftp.nlst(f"{agravos[disease]}BR*.dbc"):
        if return_path:
            dbcs.append(f"{fpath}/{dbc}")
        else:
            dbcs.append(dbc)
        
    ftp.cwd(ppath)
    logger.debug(f"Changing FTP work dir to: {ppath}")
    for dbc in ftp.nlst(f"{agravos[disease]}BR*.dbc"):
        if return_path:
            dbcs.append(f"{ppath}/{dbc}")
        else:
            dbcs.append(dbc)

    return dbcs


def download(disease, year, return_chunks=False, data_path="/tmp/pysus"):
    """
    Downloads SINAN data directly from Datasus ftp server.
    :param disease: Disease according to `agravos`.
    :param year: 4 digit integer.
    :param return_chunks: If set to True, download the data in parquet chunks.
    :param data_path: The directory where the chunks will be downloaded to.
    @note The data will be downloaded either return_chunks is set True or False,
          the difference between the two is that setting to False will read the
          parquet chunks, return as a DataFrame and clean after read.
    :return: Default behavior returns a Pandas DataFrame.
    """
    disease = check_case(disease)
    year2 = str(year)[-2:].zfill(2)
    dis_code = agravos[disease]
    fname = f"{dis_code}BR{year2}.dbc"
    years = get_available_years(disease) #legacy
    
    #Returns a list with all the DBC files found with their path,
    # enabling the user to download all the DBCs available in both
    # FINAIS and PRELIM directories 
    fyears = get_available_years(disease, return_path=True)

    first_year = [f.split(".")[0][-2:] for f in years][
        0
    ]

    if not years or fname not in years:
        raise Exception(f"No data found for this request. Available data for {disease}: \n{years}")

    if year2 < first_year: #legacy
        raise ValueError(f"SINAN does not contain data before {first_year}")

    logger.warning(
        "Now SINAN tables are no longer split by state. Returning country table" 
    ) #legacy
    #Generate the path to be downloaded from the FTP Server
    pname = next(p for p in fyears if fname in p)
    sus_path = "/".join(pname.split("/")[:-1])

    #Create the path where the data will be downloaded locally
    data_path = Path(data_path)
    data_path.mkdir(exist_ok=True, parents=True)
    logger.debug(f"{data_path} directory created.")

    out = Path(data_path) / fname
    dbf = Path(f"{str(out)[:-4]}.dbf")

    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    logger.debug(f"Stablishing connection with ftp.datasus.gov.br.\n{ftp.welcome}")

    if not Path(out).exists():
        logger.debug(f"{fname} file not found. Proceeding to download..")
        try:
            _fetch_file(fname, sus_path, "DBC", return_df=False, data_path=data_path)
            logger.info(f"{fname} downloaded at {data_path}")

        except Exception as e:
            logger.error(e)

    try:
        partquet_dir = chunk_dbfiles_into_parquets(str(out))

        if not return_chunks:
            df = parquets_to_dataframe(partquet_dir, clean_after_read=True)
            return df

        return partquet_dir

    except Exception as e:
        logger.error(e)

    finally:
        out.unlink(missing_ok=True)
        dbf.unlink(missing_ok=True)
        Path(fname).unlink(missing_ok=True)
        Path(f'{fname[:-4]}.dbf').unlink(missing_ok=True)
        logger.debug("🧹 Cleaning data residues")


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

    available_years = get_available_years(disease, return_path=True)

    if available_years:
        for dbc in available_years:
            year = dbc.split('.dbc')[0][-2:]

            parquet_dir = download(
                disease = disease, 
                year = year, 
                return_chunks = True,
                data_path = data_dir
            )

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
                f"Disease {disease.title()} is not available in SINAN.\n"
                "Available diseases: {list_diseases()}"
            )
    return disease
