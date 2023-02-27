import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ftplib import FTP
from dbfread import DBF
from pathlib import Path
from typing import List, Union
from sqlalchemy import VARCHAR, DATE, NUMERIC, INTEGER

from .diseases import DISEASE_CODE
from .typecast import COLUMN_TYPE
from pysus.utilities.readdbc import dbc2dbf


class SINAN:
    """
    Usage:
    - SINAN.diseases
    - SINAN.available_years('Zika')
    - SINAN.download_parquets('Zika', years = [19, 20, 21])
    - SINAN.parquets_to_df('Zika', 19)
    """

    diseases = list(DISEASE_CODE.keys())

    def available_years(self, disease: str, stage: str = 'all') -> list:
        return Disease(disease).get_years(stage)

    def download_parquets(
        disease: str,
        years: List[Union[(int, str)]] = None,
        data_path: str = '/tmp/pysus',
    ):
        _disease = Disease(disease)
        ftp = FTP('ftp.datasus.gov.br')

        if not years:
            _years = _disease.get_years()
        else:
            _years = years

        _paths = _disease.get_ftp_paths(_years)
        Path(data_path).mkdir(parents=True, exist_ok=True)

        for path in _paths:
            filename = str(path).split('/')[-1]
            filepath = Path(data_path) / filename
            parquet_dir = f'{str(filepath)[:-4]}.parquet'
            Path(parquet_dir).mkdir(exist_ok=True, parents=True)
            if not any(os.listdir(parquet_dir)):
                ftp.login()
                ftp.retrbinary(f'RETR {path}', open(filepath, 'wb').write)
                parquet_dir = _dbc_to_parquet_chunks(str(filepath))
            print(f'[INFO] {_disease} at {parquet_dir}')

    def parquets_to_df(
        disease: str, year: Union[(str, int)], data_path='/tmp/pysus'
    ):
        dis = Disease(disease)
        _year = str(year)[-2:].zfill(2)
        parquet_dir = Path(data_path) / f'{dis.code}BR{_year}.parquet'

        if parquet_dir.exists() and any(os.listdir(parquet_dir)):
            chunks = parquet_dir.glob('*.parquet')
            chunks_df = [
                _convert_df_types(
                    pd.read_parquet(str(f), engine='fastparquet')
                )
                for f in chunks
            ]
            df = pd.concat(chunks_df, ignore_index=True)
            objs = df.select_dtypes(object)
            df[objs.columns] = objs.apply(lambda x: x.str.replace('\x00', ''))
            return df

    def metadata_df(disease: str):
        code = DISEASE_CODE[disease]
        metadata_file = (
            Path(__file__).parent.parent.parent
            / 'metadata'
            / 'SINAN'
            / f'{code}.tar.gz'
        )
        df = pd.read_csv(
            metadata_file,
            compression='gzip',
            header=0,
            sep=',',
            quotechar='"',
            error_bad_lines=False,
        )

        return df.iloc[:, 1:]


class Disease:
    name: str

    def __init__(self, name: str) -> None:
        self.name = self.__diseasecheck__(name)

    def __diseasecheck__(self, name: str) -> str:
        return (
            name
            if name in DISEASE_CODE.keys()
            else ValueError(f'{name} not found.')
        )

    def __repr__(self) -> str:
        return f'SINAN Disease ({self.name})'

    def __str__(self) -> str:
        return self.name

    @property
    def code(self) -> str:
        return DISEASE_CODE[self.name]

    def get_years(self, stage: str = 'all') -> list:
        """
        Returns the available years to download, if no stage
        is passed, it will return years from both finals and
        preliminaries datasets.
        stage (str): 'finais' | 'prelim' | 'all'
        """

        extract_years = lambda paths: [
            str(path).split('/')[-1].split('.dbc')[0][-2:] for path in paths
        ]

        p = _ftp_list_datasets_paths
        prelim_years = extract_years(p(self.name, 'prelim'))
        finais_years = extract_years(p(self.name, 'finais'))

        if stage == 'prelim':
            return sorted(prelim_years)
        elif stage == 'finais':
            return sorted(finais_years)
        return sorted(prelim_years + finais_years)

    def get_ftp_paths(self, years: list) -> list:
        """
        Returns the FTP path available for years to download.
        years (list): a list with years to download, if year
                      is not available, it won't be included
                      in the result
        """
        p = _ftp_list_datasets_paths
        prelim_paths = p(self.name, 'prelim')
        finais_paths = p(self.name, 'finais')
        all_paths = prelim_paths + finais_paths
        ds_paths = list()
        mask = lambda _year: str(_year)[-2:].zfill(2)
        for year in years:
            [ds_paths.append(path) for path in all_paths if mask(year) in path]

        return ds_paths


def _ftp_list_datasets_paths(disease: str, stage: str) -> list:
    """
    stage: 'f'|'finais' or 'p'|'prelim'
    """
    datasets_path = '/dissemin/publicos/SINAN/DADOS/'

    if stage.startswith('f'):
        datasets_path += 'FINAIS'
    elif stage.startswith('p'):
        datasets_path += 'PRELIM'
    else:
        raise ValueError(f'{stage}')

    code = DISEASE_CODE[disease]

    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    ftp.cwd(datasets_path)
    available_dbcs = ftp.nlst(f'{code}BR*.dbc')

    return [f'{ftp.pwd()}/{dbc}' for dbc in available_dbcs]


def _dbc_to_parquet_chunks(dbcfilepath: str) -> str:
    """
    Converts .dbc file to parquet chunks, removing the leftover files.
    Returns the parquet dir path.
    """
    dbffilepath = f'{dbcfilepath[:-4]}.dbf'
    parquetpath = f'{dbcfilepath[:-4]}.parquet'

    dbc2dbf(dbcfilepath, dbffilepath)
    Path(dbcfilepath).unlink()

    for d in _stream_DBF(DBF(dbffilepath, encoding='iso-8859-1', raw=True)):
        try:
            df = pd.DataFrame(d).applymap(
                lambda x: x.decode(encoding='iso-8859-1')
                if isinstance(x, bytes)
                else x
            )
            parquet = pa.Table.from_pandas(df)
            pq.write_to_dataset(parquet, root_path=parquetpath)

        except Exception as e:
            raise e

    Path(dbffilepath).unlink()
    return parquetpath


def _convert_df_types(df: pd.DataFrame) -> pd.DataFrame:
    """Converts each column to its properly data types"""
    for column in df.columns:
        if column in COLUMN_TYPE.keys():
            try:
                sql_type = COLUMN_TYPE[column]
                if sql_type is VARCHAR:
                    df = df.astype(dtype={column: 'string'})
                elif sql_type is NUMERIC or INTEGER:
                    df[column] = pd.to_numeric(df[column])
                elif sql_type is DATE:
                    df[column] = pd.to_datetime(df[column])
            except Exception:
                df = df.astype(dtype={column: 'object'})

    return df


def _stream_DBF(dbf, chunk_size=30000):
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
