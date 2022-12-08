import unittest

import pandas as pd
from pysus.online_data.Infodengue import (
    download,
    geocode_by_cities,
    search_cities,
)


class InfoDengueTestCase(unittest.TestCase):
    def test_search_cities(self):
        city_name = search_cities("Rio de Janeiro")
        expected_geocode = 3304557
        pattern_city_names = dict(
            search_cities(dict=geocode_by_cities, city_name="Rio de")
        )
        math_cities = {"Rio de Contas": 2926707, "Rio de Janeiro": 3304557}

        self.assertEqual(expected_geocode, city_name)
        self.assertEqual(math_cities, pattern_city_names)

    def test_download(self):
        df = download(
            "dengue",
            202101,
            202152,
            "Rio de Janeiro",
        )
        df_size = (29, 52)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
        self.assertEqual(df_size, df.shape)


if __name__ == "__main__":
    unittest.main()
