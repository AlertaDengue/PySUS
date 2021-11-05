__author__ = "fccoelho"

import unittest

import pandas as pd

from pysus.online_data.CIHA import download

unittest.skip("too slow to run om travis")


class SIHTestCase(unittest.TestCase):
    def test_download_CIH(self):
        df = download("mg", 2009, 7)

        self.assertGreater(len(df), 0)
        self.assertIn("DIAG_PRINC", df.columns)
        self.assertIsInstance(df, pd.DataFrame)

    def test_download_CIHA(self):
        df = download("MG", 2013, 10)
        self.assertGreater(len(df), 0)
        self.assertIn("DIAG_PRINC", df.columns)
        self.assertIsInstance(df, pd.DataFrame)


if __name__ == "__main__":
    unittest.main()
