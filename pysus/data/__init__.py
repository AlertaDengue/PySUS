import os
from pathlib import Path

from tqdm import tqdm
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dbfread import DBF
from pyreaddbc import dbc2dbf


def dbc_to_dbf(dbc: str) -> str:
    path = Path(dbc)

    if path.suffix.lower() != ".dbc":
        raise ValueError(f"Not a DBC file: {path}")

    dbf = path.with_suffix(".dbf")

    if dbf.exists():
        return str(dbf)

    dbc2dbf(str(path), str(dbf))
    path.unlink()

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


def dbf_to_parquet(dbf: str) -> str:
    path = Path(dbf)

    if path.suffix.lower() != ".dbf":
        raise ValueError(f"Not a DBF file: {path}")

    parquet = path.with_suffix(".parquet")

    if parquet.exists():
        return str(parquet)

    parquet.absolute().mkdir()

    approx_final_size = os.path.getsize(path) / 200
    with tqdm(total=approx_final_size, unit='B', unit_scale=True) as pbar:
        pbar.set_description("DBF to Parquets")
        try:
            chunk_size = 30_000
            for chunk in stream_dbf(
                DBF(path, encoding="iso-8859-1", raw=True), chunk_size
            ):
                chunk_df = pd.DataFrame(chunk)
                table = pa.Table.from_pandas(chunk_df)
                pq.write_to_dataset(table, root_path=str(parquet))
                pbar.update(chunk_size)
        except Exception as exc:
            parquet.absolute().unlink()
            raise exc

        pbar.update(approx_final_size - pbar.n)

    return str(parquet)
