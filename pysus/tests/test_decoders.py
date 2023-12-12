# -*- coding:utf-8 -*-
u"""
Created on 19/07/16
by fccoelho
license: GPL V3 or Later
"""

import unittest
import pytest

import numpy as np
import pandas as pd
from numpy.testing import *

from pysus.online_data.SIM import download, get_CID10_chapters_table
from pysus.preprocessing import decoders
from pysus.preprocessing.SIM import (
    group_and_count,
    redistribute_cid_chapter,
    redistribute_missing,
)


def get_CID10_code(index, code):
    try:
        code = index[code]
    except KeyError:
        code = -1
    return code


class TestDecoder(unittest.TestCase):
    @pytest.mark.timeout(10)
    def test_decodifica_idade_retorna_em_anos(self):
        res = decoders.decodifica_idade_SINAN(4010, unidade="Y")
        self.assertEqual(res, 10)
        res = decoders.decodifica_idade_SINAN(3120, unidade="Y")
        self.assertEqual(res, 10)
        res = decoders.decodifica_idade_SINAN(2365, unidade="Y")
        self.assertAlmostEqual(res, 1, places=4)
        res = decoders.decodifica_idade_SINAN(1480, unidade="Y")
        self.assertAlmostEqual(res, 0.0547, places=3)

    @pytest.mark.timeout(10)
    def test_decodifica_lista_idades_retorna_em_anos(self):
        res = decoders.decodifica_idade_SINAN([4010] * 3, unidade="Y")
        assert_array_equal(res, np.array([10] * 3))
        res = decoders.decodifica_idade_SINAN([3120] * 4, unidade="Y")
        assert_array_equal(res, np.array([10] * 4))
        res = decoders.decodifica_idade_SINAN([2365] * 2, unidade="Y")
        assert_array_equal(res, np.array([1, 1]))
        res = decoders.decodifica_idade_SINAN([1480] * 5, unidade="Y")
        assert_array_almost_equal(res, np.array([0.0547] * 5), decimal=3)

    @pytest.mark.timeout(10)
    def test_decodifica_idade_retorna_em_anos_SIM(self):
        res = decoders.decodifica_idade_SIM(["501"], unidade="Y")
        assert_array_equal(res, np.array([101]))
        res = decoders.decodifica_idade_SIM(["401"] * 2, unidade="Y")
        assert_array_equal(res, np.array([1] * 2))
        res = decoders.decodifica_idade_SIM(["311"] * 3, unidade="Y")
        assert_array_almost_equal(res, np.array([0.904109589] * 3), decimal=3)
        res = decoders.decodifica_idade_SIM(["224"] * 4, unidade="Y")
        assert_array_almost_equal(res, np.array([0.065753425] * 4), decimal=3)
        res = decoders.decodifica_idade_SIM(["130"] * 5, unidade="Y")
        assert_array_almost_equal(res, np.array([0.00274] * 5), decimal=3)
        res = decoders.decodifica_idade_SIM(["010"] * 6, unidade="m")
        assert_array_almost_equal(res, np.array([10.0] * 6))

    @pytest.mark.timeout(10)
    def test_verifica_geocodigo(self):
        self.assertTrue(decoders.is_valid_geocode(3304557))

    @pytest.mark.timeout(60)
    def test_translate_variables(self):
        df = download(groups="cid10", states="sp", years=2010).to_dataframe()
        df = decoders.translate_variables_SIM(df)
        sex_array = set(df["SEXO"].unique().tolist())
        assert sex_array <= set(["Masculino", "Feminino", "NA"])
        raca_array = set(df["RACACOR"].unique().tolist())
        assert raca_array <= set(
            ["Branca", "Preta", "Amarela", "nan", "Parda", "Indígena", "NA"])

    @pytest.mark.timeout(60)
    def test_get_cid_chapter(self):
        code_index = decoders.get_CID10_code_index(get_CID10_chapters_table())
        test_causes = pd.DataFrame(
            {
                "causas": [
                    "A00",
                    "B99",
                    "D48",
                    "D49",
                    "D50",
                    "H00",
                    "H59",
                    "H60",
                    "V00",
                    "W00",
                    "X00",
                    "U00",
                    "U04",
                ]
            }
        )
        results = test_causes["causas"].map(
            lambda x: get_CID10_code(code_index, x)
        )
        assert_array_equal(
            results, [1, 1, 2, -1, 3, 7, 7, 8, -1, 20, 20, -1, 22])

    @pytest.mark.timeout(60)
    def test_group_and_count(self):
        df = download(groups="cid10", states="se", years=2010).to_dataframe()
        df = decoders.translate_variables_SIM(df)
        variables = ["CODMUNRES", "SEXO", "IDADE_ANOS"]
        counts = group_and_count(df, variables)
        sample = (
            counts[counts["COUNTS"] != 0]["COUNTS"].sample(
                20, random_state=0).tolist()
        )
        self.assertGreater(sum(sample), 0)

    @pytest.mark.skip(reason="This test takes too long")
    def test_redistribute(self):
        df = download(groups="cid10", states="sp", years=2010).to_dataframe()
        df = decoders.translate_variables_SIM(
            df, age_classes=True, classify_cid10_chapters=True
        )
        variables = ["CODMUNRES", "SEXO", "IDADE_ANOS", "CID10_CHAPTER"]
        df = df[variables]
        counts = group_and_count(df, variables)
        sum_original = counts["COUNTS"].sum()
        counts = redistribute_missing(counts, variables)
        sum_redistributed = counts["COUNTS"].sum()

        assert_almost_equal(sum_original, sum_redistributed, 10)

        sample = (
            counts[counts["COUNTS"] != 0]["COUNTS"].sample(
                20, random_state=0).tolist()
        )
        assert len(sample) == 20

        counts = redistribute_cid_chapter(
            counts, ["CODMUNRES", "SEXO", "IDADE_ANOS"])
        sum_redistributed = counts["COUNTS"].sum()

        assert_almost_equal(sum_original, sum_redistributed, 10)

        sample = (
            counts[counts["COUNTS"] != 0]["COUNTS"].sample(
                20, random_state=0).tolist()
        )
        assert len(sample) == 20
