__author__ = "fccoelho"

import unittest
import pytest

import pandas as pd

from pysus.online_data.CIHA import download
from pysus.online_data import parquets_to_dataframe

unittest.skip("too slow to run om travis")


class SIHTestCase(unittest.TestCase):
    @pytest.mark.skip(reason="This test takes too long")
    @pytest.mark.timeout(5)
    def test_download_CIH(self):
        files = download("mg", 2011, 7)
        df = parquets_to_dataframe(files)
        self.assertGreater(len(df), 0)
        self.assertIn("DIAG_PRINC", df.columns)
        self.assertIsInstance(df, pd.DataFrame)

    @pytest.mark.skip(reason="This test takes too long")
    @pytest.mark.timeout(5)
    def test_download_CIHA(self):
        files = download("MG", 2013, 10)
        df = parquets_to_dataframe(files)
        self.assertGreater(len(df), 0)
        self.assertIn("DIAG_PRINC", df.columns)
        self.assertIsInstance(df, pd.DataFrame)


if __name__ == "__main__":
    unittest.main()
