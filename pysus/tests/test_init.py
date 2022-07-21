import unittest

import pandas as pd
from numpy import dtype

from pysus.online_data import *


class TestInitFunctions(unittest.TestCase):
    def test_last_update(self):
        df = last_update('SINAN')
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(df.size, 0)
        self.assertIn('folder', df.columns)
        self.assertIsInstance(df['date'][0], pd.Timestamp)
        self.assertEqual(df.file_size.dtype, dtype('int64'))


if __name__ == '__main__':
    unittest.main()
