"""
Created on 17/08/16
by fccoelho
license: GPL V3 or Later
"""

import unittest
from pathlib import Path

import pandas as pd
from pysus.utilities.readdbc import read_dbc, read_dbc_dbf

PATH_ROOT = Path(__file__).resolve().parent
TEST_DATA = PATH_ROOT / "test_data"


class TestReadDbc(unittest.TestCase):
    dbf_fname = TEST_DATA / "EPR-2016-06-01-2016.dbf"
    dbc_fname = TEST_DATA / "sids.dbc"

    @unittest.skip('Issue #111')
    def test_read_dbc(self):
        df = read_dbc(str(self.dbc_fname))
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(df.size, 0)

    def test_read_dbf(self):
        df = read_dbc_dbf(str(self.dbf_fname))
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(df.size, 0)
