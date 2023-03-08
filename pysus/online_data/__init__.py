"""
Created on 21/09/18
by fccoelho
license: GPL V3 or Later
"""
import logging
import os
import re
import shutil
from ftplib import FTP
from datetime import datetime
from pathlib import Path, PosixPath
from typing import Union
from pysus._classes.sinan.diseases import DISEASE_CODE

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dbfread import DBF
from pysus.utilities.readdbc import dbc2dbf, read_dbc

CACHEPATH = os.getenv(
    "PYSUS_CACHEPATH", os.path.join(str(Path.home()), "pysus")
)

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


def _fetch_file(
    fname: str,
    path: str,
    ftype: str,
    return_df: bool = True,
    data_path: str = "/tmp/pysus",
) -> pd.DataFrame:
    """
    Fetch a single file.
    :param fname: Name of the file
    :param path: ftp path where file is located
    :param ftype: 'DBC' or 'DBF'
    :return:
    Pandas Dataframe
    """
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    ftp.cwd(path)

    Path(data_path).mkdir(exist_ok=True)

    try:
        ftp.retrbinary(
            f"RETR {fname}", open(f"{Path(data_path) / fname}", "wb").write
        )
    except Exception:
        raise Exception("File {} not available on {}".format(fname, path))
    if return_df:
        df = get_dataframe(fname, ftype, data_path)
        return df
    else:
        return pd.DataFrame()


def get_dataframe(
    fname: str, ftype: str, data_path: str = "/tmp/pysus"
) -> pd.DataFrame:
    """
    Return a dataframe read fom temporary file on disk.
    :param fname: temporary file name
    :param ftype: 'DBC' or 'DBF'
    :return:  DataFrame
    """
    fname = Path(data_path) / fname

    if ftype == "DBC":
        df = read_dbc(fname, encoding="iso-8859-1", raw=False)
    elif ftype == "DBF":
        dbf = DBF(fname, encoding="iso-8859-1", raw=False)
        df = pd.DataFrame(list(dbf))
    if os.path.exists(fname):
        os.unlink(fname)
    df.applymap(
        lambda x: x.decode("iso-8859-1") if isinstance(x, bytes) else x
    )
    return df


def chunk_dbfiles_into_parquets(fpath: str) -> str(PosixPath):

    dbfile = str(Path(fpath).absolute()).split("/")[-1]

    if Path(dbfile).suffix in [".dbc", ".DBC"]:
        outpath = f"{fpath[:-4]}.dbf"

        try:
            dbc2dbf(fpath, outpath)

        except Exception as e:
            logging.error(e)

        fpath = outpath

    parquet_dir = f"{fpath[:-4]}.parquet"
    if not Path(parquet_dir).exists():
        Path(parquet_dir).mkdir(exist_ok=True, parents=True)
        for d in stream_DBF(DBF(fpath, encoding="iso-8859-1", raw=True)):
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

        logging.info(f"{fpath} chunked into parquets at {parquet_dir}")

    return parquet_dir


def parquets_to_dataframe(
    parquet_dir: str(PosixPath), clean_after_read=False
) -> pd.DataFrame:

    parquets = Path(parquet_dir).glob("*.parquet")

    try:
        chunks_list = [
            pd.read_parquet(str(f), engine="fastparquet") for f in parquets
        ]

        return pd.concat(chunks_list, ignore_index=True)

    except Exception as e:
        logging.error(e)

    finally:
        if clean_after_read:
            shutil.rmtree(parquet_dir)
            logging.info(f"{parquet_dir} removed")


def stream_DBF(dbf, chunk_size=30000):
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


def get_CID10_table(cache=True):
    """
    Fetch the CID10 table
    :param cache:
    :return:
    """
    fname = "CID10.DBF"
    cachefile = os.path.join(
        CACHEPATH, "SIM_" + fname.split(".")[0] + "_.parquet"
    )
    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df
    df = _fetch_file(fname, "/dissemin/publicos/SIM/CID10/TABELAS", "DBF")
    if cache:
        df.to_parquet(cachefile)
    return df


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


def last_update(database: str = "SINAN") -> pd.DataFrame:
    """
    Return the date of last update from the database specified.

    Parameters
    ----------
    database: Database to check
    """
    if database not in DB_PATHS:
        print(
            f"Database {database} not supported try one of these"
            "{list(DB_PATHS.keys())}"
        )
        return pd.DataFrame()

    with FTP("ftp.datasus.gov.br") as ftp:
        ftp.login()
        response = {"folder": [], "date": [], "file_size": [], "file_name": []}

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

        for pth in DB_PATHS[database]:
            ftp.cwd(pth)
            flist = ftp.retrlines("LIST", parse)
    return pd.DataFrame(response)


class FTP_Inspect:

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

    def list_all_dbcs(
        self,
        SINAN_disease: str = None,
        SIA_group: str = None,
        SIH_group: str = "RD",
        CNES_group: str = None,
        PNI_group: str = "CPNI",
    ) -> list:
        available_dcs = list()
        for path in self._ds_paths:
            try:
                ftp = FTP("ftp.datasus.gov.br")
                ftp.login()
                if self.database == "CNES":
                    if not CNES_group:
                        raise ValueError(f"No group assigned to CNES_group")
                    available_dcs.extend(
                        ftp.nlst(f"{path}/{CNES_group}/*.DBC")
                    )
                elif self.database == "SIA":
                    if not SIA_group:
                        raise ValueError(f"No group assigned to SIA_group")
                    available_dcs.extend(ftp.nlst(f"{path}/{SIA_group}*.DBC"))
                elif self.database == "SIH":
                    if not SIH_group:
                        raise ValueError(f"No group assigned to SIH_group")
                    available_dcs.extend(ftp.nlst(f"{path}/{SIH_group}*.DBC"))
                elif self.database == "PNI":
                    if not PNI_group:
                        raise ValueError(f"No group assigned to PNI_group")
                    available_dcs.extend(ftp.nlst(f"{path}/{PNI_group}*.DBF"))
                elif self.database == "SINAN":
                    if not SINAN_disease:
                        raise ValueError(
                            f"No disease assigned to SINAN_disease"
                        )
                    disease = SINAN_Disease(SINAN_disease)
                    available_dcs = disease.get_ftp_paths(
                        disease.get_years(stage="all")
                    )
                else:
                    available_dcs.extend(
                        ftp.nlst(f"{path}/*.DBC")  # case insensitive
                    )
            except Exception as e:
                raise e
            finally:
                ftp.close()
        return available_dcs

    def get_available_years(
        self,
        UF: str = None,
        SIA_group: str = None,
        SIH_group: str = "RD",
        CNES_group: str = None,
        PNI_group: str = "CPNI",
        SINAN_disease: str = None,
    ):
        if UF is not None and len(UF) > 2:
            raise ValueError("Use UF abbreviation. Eg: RJ")
        available_years = list()
        all_dbcs = self.list_all_dbcs()
        filename = lambda x: str(x).split("/")[-1].upper().split(".DBC")[0]
        list_available_years = lambda group_prefix: [
            available_years.append(filename(path)[-2:])
            for path in all_dbcs
            if UF in filename(path)[len(group_prefix) :]
        ]
        if self.database == "SINAN":
            if not SINAN_disease:
                raise ValueError("No disease assigned to SINAN_disease")
            dis = SINAN_Disease(SINAN_disease)
            available_years = dis.get_years(stage="all")
        elif self.database == "SIM":
            for path in all_dbcs:
                if "/CID9/" in path:
                    available_years.append(filename(path)[-2:]) if str(path)[
                        -8:-6
                    ] == UF else None
                elif "/CID10/" in path:
                    available_years.append(filename(path)[-2:]) if str(path)[
                        -10:-8
                    ] == UF else None
        elif self.database == "SINASC":
            [
                available_years.append(filename(path)[-2:])
                for path in all_dbcs
                if UF in filename(path)[2:-4]
            ]
        elif self.database == "SIA":
            all_dbcs = self.list_all_dbcs(SIA_group)
            list_available_years(SIA_group)
        elif self.database == "SIH":
            all_dbcs = self.list_all_dbcs(SIH_group)
            list_available_years(SIH_group)
        elif self.database == "PNI":
            list_available_years(PNI_group)
        elif self.database == "CNES":
            all_dbcs = self.list_all_dbcs(CNES_group)
            list_available_years(CNES_group)
        elif self.database == "CIHA":
            available_years = set()
            [
                available_years.add(filename(path)[-4:-2])
                for path in all_dbcs
                if UF in filename(path)[4:]
            ]

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


class FTP_Downloader:

    _ftp_db: FTP_Inspect
    dbc_paths: list = None
    cache_dir: str = CACHEPATH

    def __init__(self, database: str) -> None:
        self._ftp_db = FTP_Inspect(database)

    def download(
        self,
        UF: str = None,
        year: Union[str, int] = None,
        month: Union[str, int] = None,
        SINAN_disease: str = None,
        CNES_group: str = None,
        SIA_group: str = None,
        SIH_group: str = "RD",
        PNI_group: str = "CPNI",
        local_dir: str = cache_dir,
    ) -> str:
        dbc_paths = self._get_dbc_paths(
            UF=UF,
            year=year,
            month=month,
            SINAN_disease=SINAN_disease,
            CNES_group=CNES_group,
            SIA_group=SIA_group,
            SIH_group=SIH_group,
            PNI_group=PNI_group,
        )

        downloaded_parquets = []

        for path in dbc_paths:
            local_path = self._extract_dbc(path, local_dir=local_dir)
            if local_path.upper().endswith("DBC"):
                dbf = local_path[-3:] + "dbf"
                dbc2dbf(local_path, dbf)
                Path(local_path).unlink()
                local_path = dbf

            parquet_dir = f"{local_path[:-4]}.parquet"
            Path(parquet_dir).mkdir(exist_ok=True, parents=True)

            for d in stream_DBF(
                DBF(local_path, encoding="iso-8859-1", raw=True)
            ):
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

            downloaded_parquets.append(parquet_dir)
        return downloaded_parquets

    def _get_dbc_paths(
        self,
        UF: str = None,
        year: Union[str, int] = None,
        month: Union[str, int] = None,
        SINAN_disease: str = None,
        CNES_group: str = None,
        SIA_group: str = None,
        SIH_group: str = "RD",
        PNI_group: str = "CPNI",
    ) -> list:
        db = self._ftp_db.database
        list_files = self._ftp_db.list_all_dbcs
        _year = str(year)[-2:].zfill(2)
        if db == "SINAN":
            all_dbcs = list_files(SINAN_disease)
            sinan_dis = SINAN_Disease(SINAN_disease)
        elif db == "CNES":
            all_dbcs = list_files(CNES_group)
        elif db == "SIA":
            all_dbcs = list_files(SIA_group)
        elif db == "SIH":
            all_dbcs = list_files(SIH_group)
        elif db == "PNI":
            all_dbcs = list_files(PNI_group)
        else:
            all_dbcs = list_files()

        if db == "SINAN":
            file_pattern = re.compile(f"{sinan_dis.code}BR{_year}\.dbc", re.I)
        elif db == "SIM" or "SINASC":
            file_pattern = re.compile(rf"[DON]+R?{UF}\d?\d?{_year}\.dbc", re.I)
        elif db == "SIH":
            file_pattern = re.compile(
                rf"{SIH_group}{UF}{_year}{month:02d}\.dbc", re.I
            )
        elif db == "SIA":
            file_pattern = re.compile(
                rf"{SIA_group}{UF}{_year}{month:02d}\.dbc", re.I
            )
        elif db == "PNI":
            file_pattern = re.compile(rf"{PNI_group}{UF}{_year}\.dbf", re.I)
        elif db == "CNES":
            file_pattern = re.compile(
                rf"{CNES_group}/{CNES_group}{UF}{_year}{month:02d}\.dbc", re.I
            )
        elif db == "CIHA":
            file_pattern = re.compile(
                rf"CIHA{UF}{_year}{month:02d}\.dbc", re.I
            )

        return list(filter(file_pattern.match, all_dbcs))

    def _extract_dbc(self, DBC_path: str, local_dir: str = cache_dir) -> str:
        filename = DBC_path.split("/")[-1]
        try:
            filepath = Path(local_dir) / filename
            ftp = FTP("ftp.datasus.gov.br")
            ftp.login()
            ftp.retrbinary(
                f"RETR {filename}",
                open(f"{filepath}", "wb").write,
            )
            return str(filepath)
        except Exception as e:
            logging.error(f"Not able to download {filename}")
            raise e
        finally:
            ftp.close()


class SINAN_Disease:
    name: str
    diseases: dict = DISEASE_CODE

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
