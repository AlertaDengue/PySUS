import unittest
import pytest

import pandas as pd

from pysus.online_data.PNI import *
from pysus.online_data import parquets_to_dataframe
 

class PNITestCase(unittest.TestCase):
    @pytest.mark.timeout(5)
    def test_get_available_years(self):
        res = get_available_years("AC")
        self.assertIsInstance(res, list)
        self.assertIn('2000', res)

    @pytest.mark.timeout(5)
    def test_get_available_docs(self):
        res = available_docs()
        self.assertIsInstance(res, list)

    @pytest.mark.skip(reason="This test takes too long")
    @pytest.mark.timeout(5)
    def test_download(self):
        files = download("RO", 2000)
        df = parquets_to_dataframe(files)
        self.assertIsInstance(df, pd.DataFrame)


if __name__ == "__main__":
    unittest.main()
