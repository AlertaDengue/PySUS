__author__ = 'fccoelho'

import unittest
from pysus.online_data.SIA import download
import pandas as pd

unittest.skip("too slow to run om travis")
class SIATestCase(unittest.TestCase):
    def test_download_after_2008(self):
        df, df2 = download('to', 2009, 12)
        self.assertGreater(len(df), 0)
        self.assertIn('PA_CODUNI', df.columns)
        self.assertIn('CODUNI', df2.columns)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIsInstance(df2, pd.DataFrame)

    def test_download_before_2008(self):
        df, _ = download('mg', 2006, 10)
        self.assertGreater(len(df), 0)
        self.assertIn('PA_CODUNI', df.columns)
        self.assertIsInstance(df, pd.DataFrame)


if __name__ == '__main__':
    unittest.main()
