import unittest
import pandas as pd
from pysus.online_data import IBGE


class SIDRA(unittest.TestCase):
    def test_get_aggregates(self):
        df = IBGE.list_agregados()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(df.size, 0)


if __name__ == '__main__':
    unittest.main()
