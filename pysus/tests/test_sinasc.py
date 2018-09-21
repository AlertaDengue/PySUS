__author__ = 'fccoelho'

import unittest
from unittest import skip
from pysus.preprocessing.sinasc import download


class TestDownload(unittest.TestCase):
    @skip
    def test_download_new(self):
        df = download('SE', 2015)
        self.assertIn('IDADEMAE', df.columns)
        self.assertGreater(len(df), 0)
    @skip
    def test_download_old(self):
        df = download('AL', 1994)
        self.assertIn('IDADE_MAE', df.columns)
        self.assertGreater(len(df), 0)


if __name__ == '__main__':
    unittest.main()
