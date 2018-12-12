__author__ = 'fccoelho'

import unittest
from pysus.online_data.SIH import download
import pandas as pd

unittest.skip("too slow to run om travis")
class SIHTestCase(unittest.TestCase):
    def test_download(self):
        df = download('to', 2009, 12)
        df2 = download('AC', 2013, 10)
        self.assertGreater(len(df), 0)
        self.assertGreater(len(df2), 0)
        self.assertIsInstance(df, pd.DataFrame)


if __name__ == '__main__':
    unittest.main()
