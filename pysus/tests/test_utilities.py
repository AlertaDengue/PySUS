u"""
Created on 17/08/16
by fccoelho
license: GPL V3 or Later
"""

import unittest

import pandas as pd

from pysus.utilities.readdbc import read_dbc, read_dbc_dbf


class TestReadDbc(unittest.TestCase):
    def test_read_dbc(self):
        df = read_dbc(b"test_data/sids.dbc")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(df.size, 0)
    def test_read_dbc_dbf(self):
        df = read_dbc_dbf("test_data/sids.dbc")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(df.size, 0)
        df = read_dbc_dbf("test_data/EPR-2016-06-01-2016.dbf")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(df.size, 0)
