# -*- coding:utf-8 -*-
u"""
Created on 19/07/16
by fccoelho
license: GPL V3 or Later
"""

import unittest

import numpy as np
from numpy.testing import *

from pysus.preprocessing import decoders


class TestDecoder(unittest.TestCase):
    def test_decodifica_idade_retorna_em_anos(self):
        res = decoders.decodifica_idade(4010, unidade='Y')
        self.assertEqual(res, 10)
        res = decoders.decodifica_idade(3120, unidade='Y')
        self.assertEqual(res, 10)
        res = decoders.decodifica_idade(2365, unidade='Y')
        self.assertAlmostEqual(res, 1, places=4)
        res = decoders.decodifica_idade(1480, unidade='Y')
        self.assertAlmostEqual(res, 0.0547, places=3)

    def test_decodifica_lista_idades_retorna_em_anos(self):
        res = decoders.decodifica_idade([4010] * 3, unidade='Y')
        assert_array_equal(res, np.array([10] * 3))
        res = decoders.decodifica_idade([3120] * 4, unidade='Y')
        assert_array_equal(res, np.array([10] * 4))
        res = decoders.decodifica_idade([2365] * 2, unidade='Y')
        assert_array_equal(res, np.array([1, 1]))
        res = decoders.decodifica_idade([1480] * 5, unidade='Y')
        assert_array_almost_equal(res, np.array([0.0547] * 5), decimal=3)

    def test_verifica_geocodigo(self):
        self.assertTrue(decoders.is_valid_geocode(3304557))
