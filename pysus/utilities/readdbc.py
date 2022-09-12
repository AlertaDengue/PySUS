"""
Created on 16/08/16
by fccoelho
license: GPL V3 or Later
"""
import csv
import gzip
import os
from tempfile import NamedTemporaryFile

import pandas as pd
from dbfread import DBF
from pyreaddbc import ffi, lib
from tqdm import tqdm


def read_dbc(filename, encoding="utf-8", raw=False):
    """
    Opens a DATASUS .dbc file and return its contents as a pandas
    Dataframe.
    :param filename: .dbc filename
    :param encoding: encoding of the data
    :param raw: |
        Skip type conversion. Set it to True to avoid type conversion errors
    :return: Pandas Dataframe.
    """
    if isinstance(filename, str):
        filename = filename.encode()
    with NamedTemporaryFile(delete=False) as tf:
        dbc2dbf(filename, tf.name.encode())
        try:
            dbf = DBF(tf.name, encoding=encoding, raw=raw)
            df = pd.DataFrame(list(dbf))
        except ValueError:
            dbf = DBF(tf.name, encoding=encoding, raw=not raw)
            df = pd.DataFrame(list(dbf))
        except Exception as e:
            print(f"Failed to read DBF: {e}")
            df = pd.DataFrame()
    os.unlink(tf.name)

    return df


def dbc2dbf(infile, outfile):
    """
    Converts a DATASUS dbc file to a DBF database saving it to `outfile`.
    :param infile: .dbc file name
    :param outfile: name of the .dbf file to be created.
    """
    if isinstance(infile, str):
        infile = infile.encode()
    if isinstance(outfile, str):
        outfile = outfile.encode()
    p = ffi.new("char[]", os.path.abspath(infile))
    q = ffi.new("char[]", os.path.abspath(outfile))

    lib.dbc2dbf([p], [q])

    # print(os.path.exists(outfile))


def read_dbc_dbf(filename: str):
    if filename.endswith(("dbc", "DBC")):
        df = read_dbc(filename, encoding="iso-8859-1")
    elif filename.endswith(("DBF", "dbf")):
        dbf = DBF(filename, encoding="iso-8859-1")
        # dbf = gpd.read_file(
        # filename, encoding="iso-8859-1"
        # ).drop("geometry", axis=1)
        df = pd.DataFrame(list(dbf))
    return df


def dbf_to_csvgz(filename: str, encoding: str = "iso-8859-1"):
    """
    Streams a dbf file to gzipped CSV file. The Gzipped csv
        will be saved on the same path but with a csv.gz extension.
    :param filename: path to the dbf file
    """
    data = DBF(filename, encoding=encoding, raw=False)
    fn = os.path.splitext(filename)[0] + ".csv.gz"

    with gzip.open(fn, "wt") as gzf:
        for i, d in tqdm(
            enumerate(data),
            desc="Converting",
        ):
            if i == 0:
                csvwriter = csv.DictWriter(gzf, fieldnames=d.keys())
                csvwriter.writeheader()
                csvwriter.writerow(d)
            else:
                csvwriter.writerow(d)
