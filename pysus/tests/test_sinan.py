import datetime
import unittest

import numpy as np
import pandas as pd

from pysus.online_data.SINAN import download, list_diseases
from pysus.preprocessing.sinan import geocode, read_sinan_dbf


class TestSINANDownload(unittest.TestCase):
    def test_download(self):
        df = download(year=2007, disease="Botulismo")
        self.assertIsInstance(df, pd.DataFrame)

    def test_fetch_viol_dom(self):
        df = download(year=2011, disease="Hantavirose")
        self.assertIsInstance(df, pd.DataFrame)

    def test_fetch_cancer_prelim(self):
        df = download(year=2022, disease="Cancer")
        self.assertIsInstance(df, pd.DataFrame)

    def test_fetch_sifilis(self):
        df = download(year=2021, disease="Sífilis Adquirida")
        self.assertIsInstance(df, pd.DataFrame)


    def test_lista_agravos(self):
        lista = list_diseases()
        self.assertIsInstance(lista, list)
        self.assertGreater(len(lista), 0)


class TestSinanDBF(unittest.TestCase):
    def test_read_dbf(self):
        df = read_sinan_dbf("test_data/EPR-2016-06-01-2016.dbf", encoding="latin-1")
        self.assertIsInstance(df, pd.DataFrame)
        for cname in df.columns:
            if cname.startswith("DT_"):
                self.assertIsInstance(df[cname][0], datetime.date)
            elif cname.startswith("SEM"):
                self.assertLessEqual(df[cname][0], 52)
                self.assertIsInstance(df[cname][0], (int, np.int64))
            elif cname.startswith(("NU", "ID")):
                if cname == "ID_AGRAVO":
                    continue
                self.assertIsInstance(
                    df[cname][0],
                    (int, float, np.int64),
                    msg="Failed on column {}, type:{}".format(
                        cname, type(df[cname][0])
                    ),
                )

    def test_type_convertion(self):
        df = read_sinan_dbf("test_data/EPR-2016-06-01-2016.dbf", encoding="latin-1")
        assert not all(df.dtypes == "object")

    def test_geocode(self):
        df = pd.read_pickle("test_data/chik.pickle")

    #  geocode(sinan_df=df, outfile='chik_2016.csv', default_city='Rio de Janeiro')


if __name__ == "__main__":
    unittest.main()
