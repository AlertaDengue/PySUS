import unittest

import pandas as pd

from pysus.online_data.CNES import download
from pysus.online_data import parquets_to_dataframe as to_df


class CNESTestCase(unittest.TestCase):
    def test_fetch_estabelecimentos(self):
        df = to_df(download(group="ST", state="SP", year=2021, month=8)[0])
        self.assertIsInstance(df, pd.DataFrame)
        # self.assertEqual(True, False)  # add assertion here

    def test_fetch_equipamentos(self):
        df = to_df(download("EQ", "RO", 2021, 9)[0])
        self.assertIsInstance(df, pd.DataFrame)


if __name__ == "__main__":
    unittest.main()
