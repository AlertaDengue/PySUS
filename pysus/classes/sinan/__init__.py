from diseases import DISEASE_CODE
from typecast import COLUMN_TYPE
from sqlalchemy import VARCHAR, DATE, NUMERIC
from ftplib import FTP
from dbfread import DBF
import os
from typing import List, Union
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from pysus.utilities.readdbc import dbc2dbf


class SINAN:
    """
    Usage:
        SINAN.diseases
        SINAN.available_years('Zika')
        SINAN.download('Zika', years = [19, 20, 21])
        or
        SINAN('Zika').download(years = [19, 20, 21])
    """

    diseases = list(DISEASE_CODE.keys())
    __ftp = FTP('ftp.datasus.gov.br')
    __disease = None
    __disease_years = None
    __data_path = '/tmp/pysus'

    def __init__(self, disease=None) -> None:
        if disease:
            self.__disease = Disease(disease)
            self.__disease_years = self.__disease.get_years('all')

    def available_years(self, disease: str = None, stage: str = 'all') -> list:
        if not disease:
            return self.__disease.get_years(stage)
        return Disease(disease).get_years(stage)

    def download(
        self,
        disease: str = None,
        years: List[Union[(int, str)]] = None,
        data_path: str = None,
    ):

        if data_path:
            self.__data_path = data_path

        if self.__disease:
            _disease = self.__disease
        else:
            _disease = Disease(disease)

        if not years:
            _years = self.__disease_years
        else:
            _years = years

        _paths = _disease.get_ftp_paths(_years)
        Path(self.__data_path).mkdir(parents=True, exist_ok=True)

        for path in _paths:
            filename = str(path).split('/')[-1]
            filepath = Path(self.__data_path) / filename
            parquet_dir = f'{str(filepath)[:-4]}.parquet'
            Path(parquet_dir).mkdir(exist_ok=True, parents=True)
            if not any(os.listdir(parquet_dir)):
                self.__ftp.login()
                self.__ftp.retrbinary(
                    f'RETR {path}', open(filepath, 'wb').write
                )
                parquet_dir = _dbc_to_parquet_chunks(str(filepath))
            print(f'[INFO] {self.__disease} at {parquet_dir}')

    def parquet_to_dataframe(self, year: Union[(str, int)]):
        dis = Disease(str(self.__disease))
        _year = str(year)[-2:].zfill(2)
        parquet_dir = Path(self.__data_path) / f'{dis.code}BR{_year}.parquet'
        if parquet_dir.exists():
            chunks = parquet_dir.glob('*.parquet')
            chunks_df = [
                _convert_df_types(
                    pd.read_parquet(str(f), engine='fastparquet')
                )
                for f in chunks
            ]
            return pd.concat(chunks_df, ignore_index=True)


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
            objs = df.select_dtypes(object)
            df[objs.columns] = objs.apply(lambda x: x.str.replace('\x00', ''))
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
            sql_type = COLUMN_TYPE[column]
            if isinstance(sql_type, VARCHAR):
                df = df.astype(dtype={column: 'string'})
            elif isinstance(sql_type, NUMERIC):
                df[column] = pd.to_numeric(df[column])
            elif isinstance(sql_type, DATE):
                df[column] = pd.to_datetime(df[column])
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
