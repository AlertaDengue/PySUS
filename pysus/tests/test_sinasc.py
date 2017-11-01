__author__ = 'fccoelho'

import unittest
from pysus.preprocessing.sinasc import download

class TestDownload(unittest.TestCase):
    def test_download(self):
        df = download('CE', 2015)
        self.assertIn('IDADEMAE', df.columns)
        self.assertGreater(len(df), 0)


if __name__ == '__main__':
    unittest.main()
