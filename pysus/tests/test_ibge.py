import unittest

import pandas as pd
import pytest
from pysus.online_data import IBGE


class SIDRA(unittest.TestCase):
    @pytest.mark.timeout(120)
    def test_get_aggregates(self):
        df = IBGE.list_agregados()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(df.size, 0)

    @pytest.mark.skip(reason="This test takes too long")
    def test_localidades_por_agregado(self):
        df = IBGE.localidades_por_agregado(475, nivel="N3")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(df.size, 0)

    @pytest.mark.timeout(120)
    @pytest.mark.skip(reason="Failing?")
    def test_lista_periodos(self):
        df = IBGE.lista_periodos(475)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(df.size, 0)

    @pytest.mark.timeout(120)
    def test_get_sidra_table(self):
        df = IBGE.get_sidra_table(
            200,
            territorial_level=6,
            geocode=4220000,
            period="last",
            classification=2,
            categories="all",
        )
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(df.size, 0)

    @pytest.mark.skip(reason="This test takes too long")
    def test_metadata(self):
        md = IBGE.metadados(475)
        self.assertIsInstance(md, dict)
        self.assertGreater(len(md), 0)

    @pytest.mark.timeout(120)
    def test_FetchData(self):
        ds = IBGE.FetchData(
            475,
            periodos=1996,
            variavel=93,
            localidades="N3[all]",
            classificacao="58[all]|2[4,5]|1[all]",
            view="flat",
        )
        self.assertIsInstance(ds, IBGE.FetchData)
        self.assertGreater(len(ds.JSON), 0)

    @pytest.mark.timeout(120)
    def test_get_population(self):
        l1 = IBGE.get_population(year=2021, source="POP")
        self.assertEqual(type(l1), pd.DataFrame)
        self.assertEqual(len(l1), 5570)
        l2 = IBGE.get_population(year=2012, source="projpop")
        self.assertEqual(type(l2), pd.DataFrame)
        self.assertEqual(len(l2), 4914)


if __name__ == "__main__":
    unittest.main()
