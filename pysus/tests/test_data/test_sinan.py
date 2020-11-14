import unittest
from pysus.online_data.SINAN import download, list_diseases, get_available_years
import pandas as pd


class TestDownload(unittest.TestCase):
    def test_get_sinan_diseases(self):
        dis = list_diseases()
        self.assertIsInstance(dis, list)
        self.assertIn('Tuberculose', dis)

    def test_get_available_years(self):
        res = get_available_years('RJ', 'dengue')
        self.assertIsInstance(res, list)
        assert res[0].startswith('DENG')

    def test_download(self):
        df = download('SP', 2018, 'Chagas')
        self.assertIsInstance(df, pd.DataFrame)





if __name__ == '__main__':
    unittest.main()
