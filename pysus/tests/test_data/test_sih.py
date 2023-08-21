__author__ = "fccoelho"

import unittest
import pytest

import pandas as pd

from pysus.online_data.SIH import download
from pysus.online_data import parquets_to_dataframe as to_df

unittest.skip("too slow to run om travis")


class SIHTestCase(unittest.TestCase):
    @pytest.mark.skip(reason="This test takes too long")
    @pytest.mark.timeout(5)
    def test_download(self):
        df = to_df(download("to", 2009, 12))
        df2 = to_df(download("AC", 2013, 10))
        self.assertGreater(len(df), 0)
        self.assertGreater(len(df2), 0)
        self.assertIsInstance(df, pd.DataFrame)


if __name__ == "__main__":
    unittest.main()
