"""
Created on 21/09/18
by fccoelho
license: GPL V3 or Later
"""
import os
import re
import shutil
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from dbfread import DBF
from typing import Union
from itertools import product
from datetime import datetime
from ftplib import FTP, error_perm
from pathlib import Path, PosixPath

from pysus.utilities.readdbc import dbc2dbf

CACHEPATH = os.getenv(
    "PYSUS_CACHEPATH", os.path.join(str(Path.home()), "pysus")
)

# create pysus cache directory
if not os.path.exists(CACHEPATH):
    os.mkdir(CACHEPATH)


DB_PATHS = {
    "SINAN": [
        "/dissemin/publicos/SINAN/DADOS/FINAIS",
        "/dissemin/publicos/SINAN/DADOS/PRELIM",
    ],
    "SIM": [
        "/dissemin/publicos/SIM/CID10/DORES",
        "/dissemin/publicos/SIM/CID9/DORES",
    ],
    "SINASC": [
        "/dissemin/publicos/SINASC/NOV/DNRES",
        "/dissemin/publicos/SINASC/ANT/DNRES",
    ],
    "SIH": [
        "/dissemin/publicos/SIHSUS/199201_200712/Dados",
        "/dissemin/publicos/SIHSUS/200801_/Dados",
    ],
    "SIA": [
        "/dissemin/publicos/SIASUS/199407_200712/Dados",
        "/dissemin/publicos/SIASUS/200801_/Dados",
    ],
    "PNI": ["/dissemin/publicos/PNI/DADOS"],
    "CNES": ["dissemin/publicos/CNES/200508_/Dados"],
    "CIHA": ["/dissemin/publicos/CIHA/201101_/Dados"],
}


def cache_contents():
    """
    List the files currently cached in ~/pysus
    :return:
    """
    cached_data = os.listdir(CACHEPATH)
    return [os.path.join(CACHEPATH, f) for f in cached_data]


def parquets_to_dataframe(
    parquet_dir: str(PosixPath), clean_after_read=False
) -> pd.DataFrame:
    """
    Receives a parquet directory path and returns it as a
    dataframe, trying to clean white spaces and convert to
    the correct data types. Can read only one parquet dir
    at time.
    """

    parquets = Path(parquet_dir).glob("*.parquet")

    try:
        chunks_list = [
            pd.read_parquet(str(f), engine="fastparquet") for f in parquets
        ]
        df = pd.concat(chunks_list, ignore_index=True)

        return _parse_dftypes(df)

    except Exception as e:
        logging.error(e)

    finally:
        if clean_after_read:
            shutil.rmtree(parquet_dir)
            logging.info(f"{parquet_dir} removed")


def _parse_dftypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse DataFrame values, cleaning blank spaces if needed
    and converting dtypes into correct types.
    """

    def str_to_int(string: str) -> Union[int, float]:
        # If removing spaces, all characters are int,
        # return int(value)
        if string.replace(" ", "").isnumeric():
            return int(string)

    if "CODMUNRES" in df.columns:
        df["CODMUNRES"] = df["CODMUNRES"].map(str_to_int)

    df = df.applymap(
        lambda x: "" if str(x).isspace() else x
    )  # Remove all space values

    df = df.convert_dtypes()
    return df


class FTP_Inspect:
    """
    Databases: "SINAN", "SIM", "SINASC", "SIH", "SIA", "PNI", "CNES", "CIHA"
    FTP_Inspect will focus mainly on enter in DataSUS ftp server
    and list the DBCs or DBFs paths for a database according to
    DB_PATH dict. Receives a Database as parameter.

    Methods
    last_update_df: Returns a DataFrame with information of the last
                    update from a database (Legacy) .

    list_available_years: Lists years found for a Database. Some DBs
                    contain groups that are needed to be passed in.

    list_all: Will list all DBC or DBF urls found on the FTP server
                    for the Database. Groups may be also required.
    """

    database: str
    _ds_paths: list
    ftp_server: FTP = FTP("ftp.datasus.gov.br")
    available_dbs: list = list(DB_PATHS.keys())

    def __init__(self, database: str) -> None:
        self.database = self.__checkdatabase__(database)
        self._ds_paths = DB_PATHS[database]

    def __checkdatabase__(self, database):
        if database not in self.available_dbs:
            raise ValueError(
                f"{database} not found"
                f" available databases: {self.available_dbs}"
            )
        return database

    def last_update_df(self) -> pd.DataFrame:  # Legacy
        """
        Return the date of last update from the database specified.

        Parameters
        ----------
        database: Database to check
        """
        if self.database not in DB_PATHS:
            print(
                f"Database {self.database} not supported try one of these"
                "{list(DB_PATHS.keys())}"
            )
            return pd.DataFrame()

        with FTP("ftp.datasus.gov.br") as ftp:
            ftp.login()
            response = {
                "folder": [],
                "date": [],
                "file_size": [],
                "file_name": [],
            }

            def parse(line):
                data = line.strip().split()
                response["folder"].append(pth)
                response["date"].append(
                    pd.to_datetime(" ".join([data[0], data[1]]))
                )
                response["file_size"].append(
                    0 if data[2] == "<DIR>" else int(data[2])
                )
                response["file_name"].append(data[3])

            for pth in DB_PATHS[self.database]:
                ftp.cwd(pth)
                flist = ftp.retrlines("LIST", parse)
        return pd.DataFrame(response)

    def list_available_years(
        self,
        UF: str = None,
        SINAN_disease: str = None,
        CNES_group: str = None,
        SIA_group: str = "PA",
        PNI_group: str = "CPNI",
        SIH_group: str = "RD",
    ):
        """
        Uses `list_all` and filters according to UF, disease (SINAN),
        or Database group if group is required.
        """
        available_years = set()
        get_filename = (
            lambda x: str(x)
            .split("/")[-1]
            .upper()
            .split(".DBC")[0]
            .split(".DBF")[0]
        )  # Trim url paths

        def list_years(
            len_group: int, fslice: slice = slice(-2, None), **kwargs
        ):
            return [
                available_years.add(get_filename(path)[fslice])
                for path in self.list_all(**kwargs)
                if UF in get_filename(path)[len_group:]
            ]

        if UF is not None and len(UF) > 2:
            raise ValueError("Use UF abbreviation. Eg: RJ")

        # SINAN
        if self.database == "SINAN":
            if not SINAN_disease:
                raise ValueError("No disease assigned to SINAN_disease")
            dis = FTP_SINAN(SINAN_disease)
            available_years = dis.get_years(stage="all")
        # SINASC
        elif self.database == "SINASC":
            list_years(2)
        # SIH
        elif self.database == "SIH":
            list_years(len(SIH_group), slice(-4, -2), SIH_group=SIH_group)

        # SIA
        elif self.database == "SIA":
            list_years(len(SIA_group), slice(-4, -2), SIA_group=SIA_group)
        # CNES
        elif self.database == "CNES":
            list_years(len(CNES_group), slice(-4, -2), CNES_group=CNES_group)
        # PNI
        elif self.database == "PNI":
            list_years(len(PNI_group), PNI_group=PNI_group)
        # CIHA
        elif self.database == "CIHA":
            list_years(4)
        # SIM
        elif self.database == "SIM":
            dbcs = self.list_all()
            available_years = set()
            for path in dbcs:
                if "/CID9/" in path:
                    available_years.add(get_filename(path)[-2:]) if str(path)[
                        -8:-6
                    ] == UF else None
                elif "/CID10/" in path:
                    available_years.add(get_filename(path)[-2:]) if str(path)[
                        -10:-8
                    ] == UF else None

        # Normalize years to {year:04d} and return sorted
        cur_year = str(datetime.now().year)[-2:]
        bef_2000 = lambda yrs: [
            "19" + y for y in yrs if y > cur_year and y <= "99"
        ]
        aft_2000 = lambda yrs: [
            "20" + y for y in yrs if y <= cur_year and y >= "00"
        ]
        return sorted(bef_2000(available_years)) + sorted(
            aft_2000(available_years)
        )

    def list_all(
        self,
        SINAN_disease: str = None,
        CNES_group: str = None,
        SIA_group: str = "PA",
        PNI_group: str = "CPNI",
        SIH_group: str = "RD",
    ) -> list:
        """
        Enters FTP server and lists all DBCs or DBFs files found for a
        Database group. Some Database require groups and SINAN DB requires
        a disease, more details can be found in their modules.
        This method will be later used to download these files into parquets
        chunks, to preserve memory, that are read using pandas and pyarrow.
        """
        available_dbs = list()
        for path in self._ds_paths:
            try:
                ftp = FTP("ftp.datasus.gov.br")
                ftp.login()
                # CNES
                if self.database == "CNES":
                    if not CNES_group:
                        raise ValueError(f"No group assigned to CNES_group")
                    available_dbs.extend(
                        ftp.nlst(f"{path}/{CNES_group}/*.DBC")
                    )
                # SIA
                elif self.database == "SIA":
                    if not SIA_group:
                        raise ValueError(f"No group assigned to SIA_group")
                    available_dbs.extend(ftp.nlst(f"{path}/{SIA_group}*.DBC"))
                # SIH
                elif self.database == "SIH":
                    if not SIH_group:
                        raise ValueError(f"No group assigned to SIH_group")
                    available_dbs.extend(ftp.nlst(f"{path}/{SIH_group}*.DBC"))
                # PNI
                elif self.database == "PNI":
                    if not PNI_group:
                        raise ValueError(f"No group assigned to PNI_group")
                    available_dbs.extend(ftp.nlst(f"{path}/{PNI_group}*.DBF"))
                # SINAN
                elif self.database == "SINAN":
                    if not SINAN_disease:
                        raise ValueError(
                            f"No disease assigned to SINAN_disease"
                        )
                    disease = FTP_SINAN(SINAN_disease)
                    available_dbs = disease.get_ftp_paths(
                        disease.get_years(stage="all")
                    )
                # SIM, SINASC
                else:
                    available_dbs.extend(
                        ftp.nlst(f"{path}/*.DBC")  # case insensitive
                    )
            except Exception as e:
                raise e
            finally:
                FTP("ftp.datasus.gov.br").close()
        return available_dbs


class FTP_Downloader:
    """
    Databases: "SINAN", "SIM", "SINASC", "SIH", "SIA", "PNI", "CNES", "CIHA"
    FTP_Downloader will be responsible for fetching DBF and DBC files
    into parquet chunks, according to a DataSUS Database (DB_PATHS).
    The main function is `download`, each Database has its specific
    url pattern, some may require a group or disease (SINAN), some may
    not require a month, year nor UF. Independent the requirements, the
    group is the only that won't accept to passed in as list. A local
    directory can be set, default dir is CACHEPATH.

    Methods
    download: Filters the files from the FTP Database according to its
              specs (UFs, Years, Months, Disease &/or Group and local dir).
              The parametes has to be set using their names in the function
              with the equals sign. It will fetch a DBC or DBF file and parse
              them into parquet chunks that will be read using pandas.
              Example:
                ciha = FTP_Downloader('CIHA')
                ufs = ['RJ', 'AC']
                years = [2022, 2023]
                months = [1, 2, 3]
                ciha.download(UFs=ufs, years=years, months=months)
    """

    _ftp_db: FTP_Inspect
    dbc_paths: list = None
    cache_dir: str = CACHEPATH

    def __init__(self, database: str) -> None:
        self._ftp_db = FTP_Inspect(database)

    def download(
        self,
        UFs: Union[str, list] = None,
        years: Union[str, int, list] = None,
        months: Union[str, int, list] = None,
        SINAN_disease: str = None,
        CNES_group: str = None,
        SIA_group: str = "PA",
        SIH_group: str = "RD",
        PNI_group: str = "CPNI",
        local_dir: str = cache_dir,
    ) -> str:
        dbc_paths = self._get_dbc_paths(
            UFs=UFs,
            years=years,
            months=months,
            SINAN_disease=SINAN_disease,
            CNES_group=CNES_group,
            SIA_group=SIA_group,
            SIH_group=SIH_group,
            PNI_group=PNI_group,
        )

        downloaded_parquets = []
        for path in dbc_paths:
            local_filepath = self._extract_dbc(path, local_dir=local_dir)
            parquet_dir = self._dbfc_to_parquets(
                local_filepath, local_dir=local_dir
            )
            downloaded_parquets.append(parquet_dir)
        return downloaded_parquets

    def _get_dbc_paths(
        self,
        UFs: Union[str, list] = None,
        years: Union[str, int, list] = None,
        months: Union[str, int, list] = None,
        SINAN_disease: str = None,
        CNES_group: str = None,
        SIA_group: str = "PA",
        SIH_group: str = "RD",
        PNI_group: str = "CPNI",
    ) -> list:
        parse_to_list = lambda ite: [ite] if not isinstance(ite, list) else ite
        UFs = parse_to_list(UFs)
        years = parse_to_list(years)
        months = parse_to_list(months)

        db = self._ftp_db.database
        list_files = self._ftp_db.list_all
        if db == "SINAN":
            all_dbcs = list_files(SINAN_disease=SINAN_disease)
            sinan_dis = FTP_SINAN(SINAN_disease)
        elif db == "CNES":
            all_dbcs = list_files(CNES_group=CNES_group)
        elif db == "SIA":
            all_dbcs = list_files(SIA_group=SIA_group)
        elif db == "SIH":
            all_dbcs = list_files(SIH_group=SIH_group)
        elif db == "PNI":
            all_dbcs = list_files(PNI_group=PNI_group)
        else:
            all_dbcs = list_files()

        def url_regex(
            month: str = None, year: str = None, UF: str = None
        ) -> re.Pattern:
            """
            Each url case is matched using regex patterns, mostly databases
            have the same file pattern, but some discrepancies can be found,
            for instance, lowercase UF and entire years and shortened years
            at the same time.
            """
            if db == "SINAN":
                if not year:
                    raise ValueError("Missing year(s)")
                file_pattern = re.compile(
                    f"{sinan_dis.code}BR{year}.dbc", re.I
                )
            elif db == "SIM" or db == "SINASC":
                if not year or not UF:
                    raise ValueError("Missing year(s) or UF(s)")
                file_pattern = re.compile(
                    rf"[DON]+R?{UF}\d?\d?{year}.dbc", re.I
                )
            elif db == "SIH":
                if not year or not month or not UF:
                    raise ValueError("Missing year(s), month(s) or UF(s)")
                file_pattern = re.compile(
                    rf"{SIH_group}{UF}{year}{month}.dbc", re.I
                )
            elif db == "SIA":
                if not year or not month or not UF:
                    raise ValueError("Missing year(s), month(s) or UF(s)")
                file_pattern = re.compile(
                    rf"{SIA_group}{UF}{year}{month}.dbc", re.I
                )
            elif db == "PNI":
                if not year or not UF:
                    raise ValueError("Missing year(s) or UF(s)")
                file_pattern = re.compile(rf"{PNI_group}{UF}{year}.dbf", re.I)
            elif db == "CNES":
                if not year or not month or not UF:
                    raise ValueError("Missing year(s), month(s) or UF(s)")
                file_pattern = re.compile(
                    rf"{CNES_group}/{CNES_group}{UF}{year}{month}.dbc", re.I
                )
            elif db == "CIHA":
                if not year or not month or not UF:
                    raise ValueError("Missing year(s), month(s) or UF(s)")
                file_pattern = re.compile(rf"CIHA{UF}{year}{month}.dbc", re.I)
            return file_pattern

        files = list()
        for y, m, uf in product(
            years or [], months or [], UFs or []
        ):  # Allows None
            norm = lambda y: str(y)[-2:].zfill(2)
            regex = url_regex(year=norm(y), month=norm(m), UF=str(uf))
            filtered = list(filter(regex.search, all_dbcs))
            files.extend(filtered)
        return files

    def _extract_dbc(self, DBC_path: str, local_dir: str = cache_dir) -> str:
        """
        Enters in the FTP server and retrieve the DBC(F) path into
        local machine.
        """
        Path(local_dir).mkdir(exist_ok=True, parents=True)
        filename = DBC_path.split("/")[-1]
        filedir = DBC_path.replace(filename, "")
        filepath = Path(local_dir) / filename
        if (
            Path(filepath).exists()
            or Path(str(filepath)[:-4] + ".parquet").exists()
        ):
            return str(filepath)
        try:
            ftp = FTP("ftp.datasus.gov.br")
            ftp.login()
            ftp.cwd(filedir)
            ftp.retrbinary(
                f"RETR {filename}",
                open(f"{filepath}", "wb").write,
            )
            return str(filepath)
        except error_perm as e:
            logging.error(f"Not able to download {filename}")
            raise e
        finally:
            ftp.close()

    def _dbfc_to_parquets(self, fpath: str, local_dir: str) -> str(PosixPath):
        """DBC/DBF files to parquets using Pandas & PyArrow"""
        db_path = Path(local_dir) / fpath
        dbfile = str(db_path.absolute()).split("/")[-1]
        if Path(dbfile).suffix in [".dbc", ".DBC"] and db_path.exists():
            outpath = f"{fpath[:-4]}.dbf"
            try:
                dbc2dbf(fpath, outpath)
                if Path(fpath).exists():
                    Path(fpath).unlink()
                fpath = outpath
            except Exception as e:
                logging.error(e)
                raise e

        parquet_dir = f"{fpath[:-4]}.parquet"
        if Path(parquet_dir).exists() and any(os.listdir(parquet_dir)):
            return parquet_dir
        Path(parquet_dir).mkdir(exist_ok=True, parents=True)
        for d in self._stream_DBF(DBF(fpath, encoding="iso-8859-1", raw=True)):
            try:
                df = pd.DataFrame(d)
                table = pa.Table.from_pandas(
                    df.applymap(
                        lambda x: x.decode(encoding="iso-8859-1")
                        if isinstance(x, bytes)
                        else x
                    )
                )
                pq.write_to_dataset(table, root_path=parquet_dir)

            except Exception as e:
                logging.error(e)

        if Path(fpath).exists():
            Path(fpath).unlink()

        return parquet_dir

    def _stream_DBF(self, dbf, chunk_size=30000):
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
            yield data


class FTP_SINAN:
    name: str
    diseases: dict = {
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

    def __init__(self, name: str) -> None:
        self.name = self.__diseasecheck__(name)

    def __diseasecheck__(self, name: str) -> str:
        return (
            name
            if name in self.diseases.keys()
            else ValueError(f"{name} not found.")
        )

    def __repr__(self) -> str:
        return f"SINAN Disease ({self.name})"

    def __str__(self) -> str:
        return self.name

    @property
    def code(self) -> str:
        return self.diseases[self.name]

    def get_years(self, stage: str = "all") -> list:
        """
        Returns the available years to download, if no stage
        is assigned, it will return years from both finals and
        preliminaries datasets.
        stage (str): 'finais' | 'prelim' | 'all'
        """

        def extract_years(paths):
            return [
                str(path).split("/")[-1].split(".dbc")[0][-2:]
                for path in paths
            ]

        p = self._ftp_list_datasets_paths
        prelim_years = extract_years(p(self.name, "prelim"))
        finais_years = extract_years(p(self.name, "finais"))

        if stage == "prelim":
            return sorted(prelim_years)
        elif stage == "finais":
            return sorted(finais_years)
        return sorted(prelim_years + finais_years)

    def get_ftp_paths(self, years: list) -> list:
        """
        Returns the FTP path available for years to download.
        years (list): a list with years to download, if year
                      is not available, it won't be included
                      in the result
        """
        p = self._ftp_list_datasets_paths
        prelim_paths = p(self.name, "prelim")
        finais_paths = p(self.name, "finais")
        all_paths = prelim_paths + finais_paths
        ds_paths = list()

        def mask(_year):
            return str(_year)[-2:].zfill(2)

        for year in years:
            [ds_paths.append(path) for path in all_paths if mask(year) in path]

        return ds_paths

    def _ftp_list_datasets_paths(self, disease: str, stage: str) -> list:
        """
        stage: 'f'|'finais' or 'p'|'prelim'
        """
        datasets_path = "/dissemin/publicos/SINAN/DADOS/"

        if stage.startswith("f"):
            datasets_path += "FINAIS"
        elif stage.startswith("p"):
            datasets_path += "PRELIM"
        else:
            raise ValueError(f"{stage}")

        code = self.diseases[disease]

        ftp = FTP("ftp.datasus.gov.br")
        ftp.login()
        ftp.cwd(datasets_path)
        available_dbcs = ftp.nlst(f"{code}BR*.dbc")

        return [f"{ftp.pwd()}/{dbc}" for dbc in available_dbcs]
