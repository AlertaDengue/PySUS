__author__ = "fccoelho"

import unittest

from pysus.online_data.sinasc import download, get_available_years


class TestDownload(unittest.TestCase):
    def test_download_new(self):
        df = download("SE", 2015)
        self.assertIn("IDADEMAE", df.columns)
        self.assertGreater(len(df), 0)

    def test_download_old(self):
        df = download("AL", 1994)
        self.assertIn("IDADE_MAE", df.columns)
        self.assertGreater(len(df), 0)

    def test_get_available_years(self):
        yrs = get_available_years("AC")
        self.assertIn("DNAC1996.DBC", yrs)
        self.assertIn("DNRAC94.DBC", yrs)


if __name__ == "__main__":
    unittest.main()
