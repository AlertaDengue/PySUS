import unittest
from pysus.online_data.CNES import download
import pandas as pd

class CNESTestCase(unittest.TestCase):
    def test_fetch_estabelecimentos(self):
        df = download(group='ST', state='SP', year=2021, month=8)
        self.assertIsInstance(df, pd.DataFrame)
        # self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
