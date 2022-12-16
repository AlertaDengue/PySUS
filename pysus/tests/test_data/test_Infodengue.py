import unittest

import pandas as pd
from pysus.online_data.Infodengue import download, search_string, geocode_by_cities, normalize


class InfoDengueTestCase(unittest.TestCase):
    def test_search_string(self):
        get_from_dict = search_string("Curitiba")
        cites_mathes = {
            "Acajutiba": 2900306,
            "Aratiba": 4300901,
            "Bacurituba": 2101350,
            "Buriti": 2102200,
            "Buriti Bravo": 2102309,
            "Buritirama": 2904753,
            "Buritirana": 2102358,
            "Buritis": 3109303,
            "Buritizal": 3508207,
            "Caatiba": 2904803,
            "Caraíbas": 2906899,
            "Carnaíba": 2603900,
            "Caturité": 2504355,
            "Craíbas": 2702355,
            "Criciúma": 4204608,
            "Cristais": 3120201,
            "Cristal": 4306056,
            "Cristina": 3120508,
            "Cromínia": 5206503,
            "Cruzília": 3120805,
            "Cuiabá": 5103403,
            "Cuitegi": 2505204,
            "Curimatá": 2203206,
            "Curitiba": 4106902,
            "Curitibanos": 4204806,
            "Curiúva": 4107009,
            "Custódia": 2605103,
            "Cutias": 1600212,
            "Duartina": 3514502,
            "Guaraíta": 5209291,
            "Guariba": 3518602,
            "Guaribas": 2204550,
            "Ibatiba": 3202454,
            "Ibicuitinga": 2305332,
            "Irituia": 1503507,
            "Itagibá": 2915205,
            "Itaituba": 1503606,
            "Itaiçaba": 2306207,
            "Itatiba": 3523404,
            "Itaíba": 2607505,
            "Itiúba": 2917003,
            "Jequitibá": 3135704,
            "Juquitiba": 3526209,
            "Marituba": 1504422,
            "Mauriti": 2308104,
            "Mucurici": 3203601,
            "Muribeca": 2804300,
            "Muritiba": 2922300,
            "Peritiba": 4212601,
            "Piritiba": 2924801,
            "Taquarituba": 3553807,
            "Tumiritinga": 3169505,
            "Turiúba": 3555208,
            "Umburatiba": 3170305,
            "Urucurituba": 1304401,
        }
        pattern_city_names = search_string(substr="r de jAiro")

        self.assertIsInstance(get_from_dict, dict)
        self.assertEqual(cites_mathes, get_from_dict)
        self.assertIn("Rio de Janeiro", pattern_city_names.keys())
        self.assertIn(4204806, get_from_dict.values() )

    def test_normalize(self):
        normalized_str = normalize("Rio das Ostras")
        
        substr_list = normalized_str.split(".")
        
        self.assertIsInstance(substr_list, list)
        # self.assertEqual(substr_list, ['rio', 'das', 'ostras'])
        self.assertEqual(normalized_str, "rio das ostras")
        
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
