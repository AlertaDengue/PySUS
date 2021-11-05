import unittest

import pandas as pd
from pandas.io.parsers import TextFileReader

from pysus.online_data.vaccine import download_covid


class VaccineTestCase(unittest.TestCase):
    def test_Download(self):
        """Careful! this download can take a long time"""
        df = download_covid("BA")
        self.assertIsInstance(df, TextFileReader)


if __name__ == "__main__":
    unittest.main()
