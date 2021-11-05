import unittest

import pandas as pd

from pysus.online_data.CNES import download


class CNESTestCase(unittest.TestCase):
    def test_fetch_estabelecimentos(self):
        df = download(group="ST", state="SP", year=2021, month=8)
        self.assertIsInstance(df, pd.DataFrame)
        # self.assertEqual(True, False)  # add assertion here

    def test_fetch_equipamentos(self):
        df = download("EQ", "RO", 2021, 9)
        self.assertIsInstance(df, pd.DataFrame)


if __name__ == "__main__":
    unittest.main()
