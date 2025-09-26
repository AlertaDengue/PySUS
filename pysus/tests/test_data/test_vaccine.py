import os
import unittest

import pandas as pd
import pytest
from pysus.online_data.vaccine import download_covid


class VaccineTestCase(unittest.TestCase):
    @pytest.mark.timeout(15)
    @unittest.skipIf(os.getenv("CI"), "Forbidden on CI")
    def test_Download(self):
        df = download_covid("BA", only_header=True)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (10000, 42))


if __name__ == "__main__":
    unittest.main()
