__author__ = "fccoelho"

import unittest

import pandas as pd

from pysus.online_data.SIA import download

unittest.skip("too slow to run om travis")


class SIATestCase(unittest.TestCase):
    def test_download_after_2008(self):
        data = download("to", 2015, 12)
        # print(data)
        self.assertGreater(len(data), 0)
        for df in data:
            if df is None:
                continue
            self.assertIn("PA_CODUNI", df.columns)
            self.assertIn("CODUNI", df.columns)
            self.assertIsInstance(df, pd.DataFrame)
            self.assertIsInstance(df, pd.DataFrame)

    def test_download_before_2008(self):
        data = download("mg", 2005, 8)
        self.assertWarns(UserWarning)
        for df in data:
            if df is None:
                continue
            self.assertGreater(len(df), 0)
            self.assertIn("PA_CODUNI", df.columns)
            self.assertIsInstance(df, pd.DataFrame)

    @unittest.expectedFailure
    def test_download_before_1994(self):
        df1, df2 = download("RS", 1993, 12)

    def test_download_one(self):
        data = download("se", 2020, 10, group="PS")

        for df in data:
            if df is None:
                continue
            self.assertGreater(len(df), 0)
            self.assertIn("CNS_PAC", df.columns)
            self.assertIsInstance(df, pd.DataFrame)

    def test_download_many(self):
        dfs = download("PI", 2018, 3, group=["aq", "AM", "atd"])
        self.assertEqual(len(dfs), 3)
        df1, df2, df3 = dfs
        self.assertIsNone(df1)
        if df1 is None:
            return
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
        dfs = download("MS", 2006, 5, group=["PA", "SAD"])
        assert len(dfs) == 2
        self.assertIsNone(dfs[0])
        self.assertIsNone(dfs[1])


if __name__ == "__main__":
    unittest.main()
