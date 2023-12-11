import os
import struct
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dbfread import DBF
from pyreaddbc import dbc2dbf


def dbc_to_dbf(dbc: str, _pbar=None) -> str:
    """
    Parses DBC files into DBFs
    """
    path = Path(dbc)

    if path.suffix.lower() != ".dbc":
        raise ValueError(f"Not a DBC file: {path}")

    dbf = path.with_suffix(".dbf")

    if _pbar:
        _pbar.reset(total=1)
        _pbar.set_description(f"{dbf.name}")

    _parquet = path.with_suffix(".parquet")
    if _parquet.exists():
        path.unlink(missing_ok=True)
        dbf.unlink(missing_ok=True)
        return str(_parquet)

    if dbf.exists():
        path.unlink(missing_ok=True)
        return str(dbf)

    dbc2dbf(str(path), str(dbf))
    path.unlink()

    if _pbar:
        _pbar.update(1)

    return str(dbf)


def stream_dbf(dbf, chunk_size=30000):
    """Fetches records in parquet chunks to preserve memory"""
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


def decode_column(value):
    """
    Decodes binary data to str
    """
    if isinstance(value, bytes):
        return value.decode(encoding="iso-8859-1").replace("\x00", "")

    if isinstance(value, str):
        return str(value).replace("\x00", "")

    return value


def dbf_to_parquet(dbf: str, _pbar=None) -> str:
    """
    Parses DBF file into parquet to preserve memory
    """
    path = Path(dbf)

    if path.suffix.lower() != ".dbf":
        raise ValueError(f"Not a DBF file: {path}")

    parquet = path.with_suffix(".parquet")

    approx_final_size = (
        os.path.getsize(path) / 200
    )  # TODO: not best approx size
    if _pbar:
        _pbar.unit = "B"
        _pbar.unit_scale = True
        _pbar.reset(total=approx_final_size)
        _pbar.set_description(f"{parquet.name}")

    if parquet.exists():
        if _pbar:
            _pbar.update(approx_final_size - _pbar.n)
        return str(parquet)

    parquet.absolute().mkdir()

    try:
        chunk_size = 30_000
        for chunk in stream_dbf(
            DBF(path, encoding="iso-8859-1", raw=True), chunk_size
        ):
            if _pbar:
                _pbar.update(chunk_size)

            chunk_df = pd.DataFrame(chunk)
            table = pa.Table.from_pandas(chunk_df.map(decode_column))
            pq.write_to_dataset(table, root_path=str(parquet))
    except struct.error as err:
        if _pbar:
            _pbar.close()
        Path(path).unlink()
        parquet.rmdir()
        raise err

    if _pbar:
        _pbar.update(approx_final_size - _pbar.n)

    path.unlink()

    return str(parquet)


def parse_dftypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse DataFrame values, cleaning blank spaces if needed
    and converting dtypes into correct types.
    """

    def map_column_func(column_names: list[str], func):
        # Maps a function to each value in each column
        columns = [c for c in df.columns if c in column_names]
        df[columns] = df[columns].map(func)

    def str_to_int(string: str):
        # If removing spaces, all characters are int,
        # return int(value). @warning it removes in between
        # spaces as well
        if str(string).replace(" ", "").isnumeric():
            return int(string.replace(" ", ""))
        return string

    def str_to_date(string: str):
        if isinstance(string, str):
            try:
                return datetime.strptime(string, "%Y%m%d").date()
            except ValueError:
                # Ignore errors, bad value
                return string
        return string

    map_column_func(["DT_NOTIFIC", "DT_SIN_PRI"], str_to_date)
    map_column_func(["CODMUNRES", "SEXO"], str_to_int)

    df = df.map(
        lambda x: "" if str(x).isspace() else x
    )  # Remove all space values

    df = df.convert_dtypes()
    return df
