import unittest
import pytest

from pysus.online_data.Infogripe import download, DATASETS


class InfoGripeTestCase(unittest.TestCase):
    @pytest.mark.skip(reason="This test takes too long")
    @pytest.mark.timeout(5)
    def test_download(self):
        for ds in DATASETS.keys():
            df = download(ds)
            self.assertGreater(len(df), 0)  # add assertion here


if __name__ == '__main__':
    unittest.main()
