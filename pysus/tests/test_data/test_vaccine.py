import unittest
import pytest

import pandas as pd

from pysus.online_data.vaccine import download_covid


class VaccineTestCase(unittest.TestCase):
    pytest.mark.timeout(5)
    def test_Download(self):
        """Careful! this download can take a long time"""
        df = download_covid("BA", only_header=True)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (10000, 42))


if __name__ == "__main__":
    unittest.main()
