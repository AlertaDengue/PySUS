import unittest

import pandas as pd
from pysus.online_data.Infodengue import (
    download,
    # geocode_by_cities,
    search_string,
)


class InfoDengueTestCase(unittest.TestCase):
    def search_string(self):
        city_name = search_cities("Curitiba")
        math_cities = {'Curitiba': 4106902, 'Curitibanos': 4204806}
        pattern_city_names = search_cities(city_name="do Sul")

        self.assertEqual(math_cities, city_name)
        self.assertIn("Jaraguá do Sul", pattern_city_names.keys())

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
