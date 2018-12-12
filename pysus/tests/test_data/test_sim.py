__author__ = 'fccoelho'

import unittest
from unittest import skip
from pysus.online_data.SIM import download


class TestDownload(unittest.TestCase):
    def test_download_CID10(self):
        df = download('ba', 2007)
        self.assertIn('IDADEMAE', df.columns)
        self.assertGreater(len(df), 0)
    def test_download_CID9(self):
        df = download('mg', 1987)
        self.assertIn('NECROPSIA', df.columns)
        self.assertGreater(len(df), 0)


if __name__ == '__main__':
    unittest.main()
