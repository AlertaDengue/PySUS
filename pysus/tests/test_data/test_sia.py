__author__ = "fccoelho"

import unittest

import pandas as pd
from pysus.online_data.SIA import download
from pysus.online_data import parquets_to_dataframe

unittest.skip("too slow to run om travis")


class SIATestCase(unittest.TestCase):
    def test_download_after_2008(self):
        files = download("to", 2015, 12)
        # print(data)
        self.assertGreater(len(files), 0)
        for file in files:
            df = parquets_to_dataframe(file)
            self.assertIn("PA_CODUNI", df.columns)
            self.assertIn("PA_GESTAO", df.columns)
            self.assertIsInstance(df, pd.DataFrame)
            self.assertIsInstance(df, pd.DataFrame)

    def test_download_before_2008(self):
        files = download("mg", 2005, 8)
        self.assertWarns(UserWarning)
        for file in files:
            df = parquets_to_dataframe(file)
            self.assertGreater(len(df), 0)
            self.assertIn("PA_CODUNI", df.columns)
            self.assertIsInstance(df, pd.DataFrame)

    @unittest.expectedFailure
    def test_download_before_1994(self):
        files = download("RS", 1993, 12)
        self.assertGreater(len(files), 0)

    def test_download_one(self):
        file = download("se", 2020, 10, group="PS")
        df = parquets_to_dataframe(file[0])
        self.assertGreater(len(df), 0)
        self.assertIn("CNS_PAC", df.columns)
        self.assertIsInstance(df, pd.DataFrame)

    def test_download_many(self):
        files = []
        groups = ["aq", "AM", "atd"]
        for group in groups:
            files.extend(download("PI", 2018, 3, group=group))
        to_df = parquets_to_dataframe    
        df1, df2, df3 = to_df(files[0]), to_df(files[1]), to_df(files[2])
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
        dfs = download("MS", 2006, 5)
        self.assertIsNotNone(dfs)


if __name__ == "__main__":
    unittest.main()
