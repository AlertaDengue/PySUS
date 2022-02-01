import unittest
from pysus.online_data.SIH import download


class SIHTestCase(unittest.TestCase):
    def test_download_pre_2008(self):
        df = download('AC', 2006, 12, cache=False)
        assert not df.empty

    def test_download_2008(self):
        df = download('SE', 2008, 6, cache=False)
        assert not df.empty

    def test_download_2010(self):
        df = download('SE', 2010, 6, cache=False)
        assert not df.empty

    def test_download_2019(self):
        df = download('SE', 2019, 6, cache=False)
        assert not df.empty


if __name__ == '__main__':
    unittest.main()
