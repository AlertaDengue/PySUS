import unittest

from pysus.online_data.ESUS import download


class MyTestCase(unittest.TestCase):
    @unittest.skip  # long test. Uncomment to run once.
    def test_download(self):
        df = download(uf="se")
        self.assertGreater(len(df), 0)


if __name__ == "__main__":
    unittest.main()
