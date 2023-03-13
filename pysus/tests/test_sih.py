import unittest

from pysus.online_data.SIH import download
from pysus.online_data import parquets_to_dataframe as to_df


@unittest.skip("Waiting for Rio de Janeiro data on database demo.")
class SIHTestCase(unittest.TestCase):
    def test_download_pre_2008(self):
        df = to_df(download("AC", 2006, 12)[0])
        assert not df.empty

    def test_download_2008(self):
        df = to_df(download("SE", 2008, 6)[0])
        assert not df.empty

    def test_download_2010(self):
        df = to_df(download("SE", 2010, 6)[0])
        assert not df.empty

    def test_download_2019(self):
        df = to_df(download("SE", 2019, 6)[0])
        assert not df.empty


if __name__ == "__main__":
    unittest.main()
