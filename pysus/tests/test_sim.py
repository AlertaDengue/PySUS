# -*- coding:utf-8 -*-
u"""
Created on 23/09/2020
by gabrielmcf
license: GPL V3 or Later
"""

import unittest

from numpy.testing import *

from pysus.online_data.SIM import download, get_CID10_chapters_table
from pysus.preprocessing import SIM, decoders

class TestDecoder(unittest.TestCase):
    def test_group_and_count(self):
        df = download('sp',2010)
        df = decoders.translate_variables_SIM(df)
        variables = ['CODMUNRES','SEXO','IDADE_ANOS']
        counts = SIM.group_and_count(df,variables)
        sample = counts[counts['CONTAGEM'] != 0]['CONTAGEM'].sample(20,random_state=0).tolist()
        assert_array_equal(sample, [1.0, 1.0, 2.0, 4.0, 9.0, 1.0, 1.0, 1.0, 3.0, 289.0, 1.0, 3.0, 3.0, 19.0, 9.0, 1.0, 2.0, 1.0, 1.0, 3.0])

    def test_redistribute(self):
        df = download('sp',2010)
        df = decoders.translate_variables_SIM(df)
        variables = ['CODMUNRES','SEXO','IDADE_ANOS']
        counts = SIM.group_and_count(df,variables)
        sum_original = counts["CONTAGEM"].sum()
        counts = SIM.redistribute(counts,variables)
        sum_redistributed = counts["CONTAGEM"].sum()

        assert_equal(sum_original,sum_redistributed)
        
        sample = counts[counts['CONTAGEM'] != 0]['CONTAGEM'].sample(20,random_state=0).tolist()
        assert_array_almost_equal(sample, [1.0026605509150972, 3.0076529330337682, 10.0, 3.0, 1.0, 7.030611240693058, 2.0, 1.0, 1.0003988761766138, 1.0, 5.0, 1.0, 2.0, 1.0, 1.0011890475332716, 1.0007766913402458, 3.0, 3.0, 1.0, 1.0], decimal=5)