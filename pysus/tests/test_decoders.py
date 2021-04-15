# -*- coding:utf-8 -*-
u"""
Created on 19/07/16
by fccoelho
license: GPL V3 or Later
"""

import unittest

import numpy as np
from numpy.testing import *

from pysus.online_data.SIM import download
from pysus.preprocessing import decoders


class TestDecoder(unittest.TestCase):
    def test_decodifica_idade_retorna_em_anos(self):
        res = decoders.decodifica_idade_SINAN(4010, unidade='Y')
        self.assertEqual(res, 10)
        res = decoders.decodifica_idade_SINAN(3120, unidade='Y')
        self.assertEqual(res, 10)
        res = decoders.decodifica_idade_SINAN(2365, unidade='Y')
        self.assertAlmostEqual(res, 1, places=4)
        res = decoders.decodifica_idade_SINAN(1480, unidade='Y')
        self.assertAlmostEqual(res, 0.0547, places=3)

    def test_decodifica_lista_idades_retorna_em_anos(self):
        res = decoders.decodifica_idade_SINAN([4010] * 3, unidade='Y')
        assert_array_equal(res, np.array([10] * 3))
        res = decoders.decodifica_idade_SINAN([3120] * 4, unidade='Y')
        assert_array_equal(res, np.array([10] * 4))
        res = decoders.decodifica_idade_SINAN([2365] * 2, unidade='Y')
        assert_array_equal(res, np.array([1, 1]))
        res = decoders.decodifica_idade_SINAN([1480] * 5, unidade='Y')
        assert_array_almost_equal(res, np.array([0.0547] * 5), decimal=3)

    def test_decodifica_idade_retorna_em_anos_SIM(self):
        res = decoders.decodifica_idade_SIM(['501'], unidade='Y')
        assert_array_equal(res, np.array([101]))
        res = decoders.decodifica_idade_SIM(['401'] * 2, unidade='Y')
        assert_array_equal(res, np.array([1] * 2))
        res = decoders.decodifica_idade_SIM(['311'] * 3, unidade='Y')
        assert_array_almost_equal(res, np.array([0.904109589] * 3), decimal=3)
        res = decoders.decodifica_idade_SIM(['224'] * 4, unidade='Y')
        assert_array_almost_equal(res, np.array([0.065753425]*4), decimal=3)
        res = decoders.decodifica_idade_SIM(['130'] * 5, unidade='Y')
        assert_array_almost_equal(res, np.array([0.00274] * 5), decimal=3)
        res = decoders.decodifica_idade_SIM(['010'] * 6, unidade='m')
        assert_array_almost_equal(res, np.array([10.] * 6))

    def test_verifica_geocodigo(self):
        self.assertTrue(decoders.is_valid_geocode(3304557))

    def test_translate_variables(self):
        df = download('sp',2010)
        df = decoders.translate_variables_SIM(df)
        sex_array = df["SEXO"].unique().tolist()
        assert_array_equal(sex_array, ['Masculino', 'Feminino', 'nan'])
        raca_array = df['RACACOR'].unique().tolist()
        assert_array_equal(raca_array, ['Branca', 'Preta', 'Amarela', 'nan', 'Parda', 'Indígena'])

    @unittest.skip
    def test_group_and_count(self):
        df = download('sp',2010)
        df = decoders.translate_variables_SIM(df)
        variables = ['CODMUNRES','SEXO','IDADE_ANOS']
        counts = decoders.group_and_count(df,variables)
        sample = counts[counts['CONTAGEM'] != 0]['CONTAGEM'].sample(20,random_state=0).tolist()
        assert_array_equal(sample, [1.0, 1.0, 2.0, 4.0, 9.0, 1.0, 1.0, 1.0, 3.0, 289.0, 1.0, 3.0, 3.0, 19.0, 9.0, 1.0, 2.0, 1.0, 1.0, 3.0])

    @unittest.skip
    def test_redistribute(self):
        df = download('sp',2010)
        df = decoders.translate_variables_SIM(df)
        variables = ['CODMUNRES','SEXO','IDADE_ANOS']
        counts = decoders.group_and_count(df,variables)
        sum_original = counts["CONTAGEM"].sum()
        counts = decoders.redistribute(counts,variables)
        sum_redistributed = counts["CONTAGEM"].sum()

        assert_equal(sum_original,sum_redistributed)
        
        sample = counts[counts['CONTAGEM'] != 0]['CONTAGEM'].sample(20,random_state=0).tolist()
        assert_array_almost_equal(sample, [1.0026605509150972, 3.0076529330337682, 10.0, 3.0, 1.0, 7.030611240693058, 2.0, 1.0, 1.0003988761766138, 1.0, 5.0, 1.0, 2.0, 1.0, 1.0011890475332716, 1.0007766913402458, 3.0, 3.0, 1.0, 1.0], decimal=5)