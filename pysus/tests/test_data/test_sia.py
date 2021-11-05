__author__ = "fccoelho"

import unittest

import pandas as pd

from pysus.online_data.SIA import download

unittest.skip("too slow to run om travis")


class SIATestCase(unittest.TestCase):
    def test_download_after_2008(self):
        df, df2 = download("to", 2009, 12)
        self.assertGreater(len(df), 0)
        self.assertIn("PA_CODUNI", df.columns)
        self.assertIn("CODUNI", df2.columns)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIsInstance(df2, pd.DataFrame)

    def test_download_before_2008(self):
        df, _ = download("mg", 2006, 10)
        self.assertWarns(UserWarning)
        self.assertGreater(len(df), 0)
        self.assertIn("PA_CODUNI", df.columns)
        self.assertIsInstance(df, pd.DataFrame)

    @unittest.expectedFailure
    def test_download_before_1994(self):
        df1, df2 = download("RS", 1993, 12)

    def test_download_one(self):
        df = download("se", 2020, 10, group="PS")
        self.assertGreater(len(df), 0)
        self.assertIn("CNS_PAC", df.columns)
        self.assertIsInstance(df, pd.DataFrame)

    def test_download_many(self):
        df1, df2, df3 = download("PI", 2018, 3, group=["aq", "AM", "atd"])
        self.assertIsInstance(df1, pd.DataFrame)
        self.assertIsInstance(df2, pd.DataFrame)
        self.assertIsInstance(df2, pd.DataFrame)
        self.assertGreater(len(df1), 0)
        self.assertGreater(len(df2), 0)
        self.assertGreater(len(df3), 0)
        self.assertIn("AP_CODUNI", df1.columns)
        self.assertIn("AP_CODUNI", df2.columns)
        self.assertIn("AP_CODUNI", df3.columns)
        self.assertIn("AQ_CID10", df1.columns)
        self.assertIn("AM_PESO", df2.columns)
        self.assertIn("ATD_CARACT", df3.columns)

    def test_download_missing(self):
        df1, df2 = download("MS", 2006, 5, group=["PA", "SAD"])
        self.assertIsInstance(df1, pd.DataFrame)
        self.assertIsNone(df2)


if __name__ == "__main__":
    unittest.main()
