import unittest

from pysus.online_data.Infogripe import download, DATASETS


class InfoGripeTestCase(unittest.TestCase):
    def test_download(self):
        for ds in DATASETS.keys():
            df = download(ds)
            self.assertGreater(len(df), 0)  # add assertion here


if __name__ == '__main__':
    unittest.main()
