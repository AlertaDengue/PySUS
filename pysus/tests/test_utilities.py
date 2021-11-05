u"""
Created on 17/08/16
by fccoelho
license: GPL V3 or Later
"""

import unittest

import pandas as pd

from pysus.utilities.readdbc import read_dbc


class TestReadDbc(unittest.TestCase):
    def test_read_dbc(self):
        df = read_dbc(b"test_data/sids.dbc")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(df.size, 0)
