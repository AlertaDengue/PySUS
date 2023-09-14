__author__ = "fccoelho"

import unittest
import pytest

from pysus.online_data.sinasc import download, get_available_years, sinasc
from pysus.online_data import parquets_to_dataframe as to_df


class TestDownload(unittest.TestCase):
    @pytest.mark.skip(reason="This test takes too long")
    @pytest.mark.timeout(5)
    def test_download_new(self):
        df = to_df(download("SE", 2015))
        self.assertIn("IDADEMAE", df.columns)
        self.assertGreater(len(df), 0)

    @pytest.mark.skip(reason="This test takes too long")
    @pytest.mark.timeout(5)
    def test_download_old(self):
        df = to_df(download("AL", 1994)[0]) #[0] bc there is a file duplicity in the ftp sever
        self.assertIn("IDADE_MAE", df.columns)
        self.assertGreater(len(df), 0)

    @pytest.mark.skip(reason="This test takes too long")
    @pytest.mark.timeout(5)
    def test_get_available_years(self):
        files = get_available_years("AC")
        self.assertIn("1996", [sinasc.format(file)[1] for file in files])


if __name__ == "__main__":
    unittest.main()
