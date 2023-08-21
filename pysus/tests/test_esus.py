import unittest
import pytest

from pysus.online_data.ESUS import download


class MyTestCase(unittest.TestCase):
    @pytest.mark.skip(reason="This test takes too long")
    @pytest.mark.timeout(5)
    def test_download(self):
        df = download(uf="se")
        self.assertGreater(len(df), 0)


if __name__ == "__main__":
    unittest.main()
