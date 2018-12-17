__author__ = 'fccoelho'

import unittest
from unittest import skip
from pysus.online_data.SIM import download, get_CID10_table, get_CID9_table, get_municipios, get_ocupations


class TestDownload(unittest.TestCase):
    def test_download_CID10(self):
        df = download('ba', 2007)
        self.assertIn('IDADEMAE', df.columns)
        self.assertGreater(len(df), 0)

    def test_download_CID9(self):
        df = download('mg', 1987)
        self.assertIn('NECROPSIA', df.columns)
        self.assertGreater(len(df), 0)

    def test_get_cid10(self):
        df = get_CID10_table()
        self.assertIn('CID10', df.columns)
        self.assertGreater(len(df), 0)

    def test_get_cid9(self):
        df = get_CID9_table()
        self.assertIn('DESCRICAO', df.columns)
        self.assertGreater(len(df), 0)

    def test_get_mun(self):
        df = get_municipios()
        self.assertIn('MUNCOD', df.columns)
        self.assertGreater(len(df), 0)

    def test_get_ocup(self):
        df = get_ocupations()
        self.assertIn('CODIGO', df.columns)
        self.assertGreater(len(df), 0)


if __name__ == '__main__':
    unittest.main()
