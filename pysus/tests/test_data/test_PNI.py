import unittest

import pandas as pd

from pysus.online_data.PNI import *


class PNITestCase(unittest.TestCase):
    def test_get_available_years(self):
        res = get_available_years("AC")
        self.assertIsInstance(res, list)
        self.assertIn("CPNIAC00.DBF", res)

    def test_get_available_docs(self):
        res = available_docs()
        self.assertIsInstance(res, list)

    def test_fetch_doc(self):
        res = available_docs()
        fetch_document(res[0])

    def test_download(self):
        df = download("RO", 2000)
        self.assertIsInstance(df, pd.DataFrame)


if __name__ == "__main__":
    unittest.main()
