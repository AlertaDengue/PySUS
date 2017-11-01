__author__ = 'fccoelho'

import unittest
from pysus.preprocessing.sinasc import download

class TestDownload(unittest.TestCase):
    def test_download_new(self):
        df = download('CE', 2015)
        self.assertIn('IDADEMAE', df.columns)
        self.assertGreater(len(df), 0)

    def test_download_old(self):
        df = download('MG', 1994)
        self.assertIn('IDADE_MAE', df.columns)
        self.assertGreater(len(df), 0)


if __name__ == '__main__':
    unittest.main()
