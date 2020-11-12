import unittest
from pysus.online_data.SINAN import download, list_diseases, get_available_years


class TestDownload(unittest.TestCase):
    def test_get_sinan_diseases(self):
        dis = list_diseases()
        self.assertIsInstance(dis, list)
        self.assertIn('Tuberculose', dis)

    def test_get_available_years(self):
        res = get_available_years('RJ', 'Dengue')
        self.assertIsInstance(res, list)
        assert res[0].startswith('DENG')





if __name__ == '__main__':
    unittest.main()
