"""
Created on 21/09/18
by fccoelho
license: GPL V3 or Later
"""
import logging
import os
import shutil
from ftplib import FTP
from pathlib import Path, PosixPath

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
    data_path: str = '/tmp/pysus'
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
        ftp.retrbinary(f"RETR {fname}", open(f'{Path(data_path) / fname}', "wb").write)
    except Exception:
        raise Exception("File {} not available on {}".format(fname, path))
    if return_df:
        df = get_dataframe(fname, ftype, data_path)
        return df
    else:
        return pd.DataFrame()


def get_dataframe(fname: str, ftype: str, data_path: str = '/tmp/pysus') -> pd.DataFrame:
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
                    lambda x: x.decode(encoding="iso-8859-1") if isinstance(x, bytes) else x
                ))
                pq.write_to_dataset(table, root_path=parquet_dir)

            except Exception as e:
                logging.error(e)

        logging.info(f"{fpath} chunked into parquets at {parquet_dir}")

    return parquet_dir


def parquets_to_dataframe(
    parquet_dir: str(PosixPath), 
    clean_after_read=False
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
    "CNES": ["dissemin/publicos/CNES/200508_/Dados/"],
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
