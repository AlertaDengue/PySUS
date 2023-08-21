import unittest
import pytest

import pandas as pd

from pysus.online_data.CNES import download
from pysus.online_data import parquets_to_dataframe as to_df


class CNESTestCase(unittest.TestCase):
    @unittest.skip('Also fails in previous versions: unpack requires a buffer of 32 bytes')
    def test_fetch_estabelecimentos(self):
        df = to_df(download(group="ST", states="SP", years=2021, months=8))
        self.assertIsInstance(df, pd.DataFrame)
        # self.assertEqual(True, False)  # add assertion here

    @pytest.mark.timeout(5)
    def test_fetch_equipamentos(self):
        df = to_df(download(group="EQ", states="RO", years=2021, months=9))
        self.assertIsInstance(df, pd.DataFrame)

