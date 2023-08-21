# -*- coding:utf-8 -*-
u"""
Created on 23/09/2020
by gabrielmcf
license: GPL V3 or Later
"""

import unittest
import pytest

from numpy.testing import (
    assert_equal,
)

from pysus.online_data.SIM import download
from pysus.preprocessing import SIM, decoders
from pysus.online_data import parquets_to_dataframe as to_df
 

class TestDecoder(unittest.TestCase):
    @pytest.mark.timeout(5)
    def test_group_and_count(self):
        df = to_df(download("se", 2010))
        df = decoders.translate_variables_SIM(df)
        variables = ["CODMUNRES", "SEXO", "IDADE_ANOS"]
        counts = SIM.group_and_count(df, variables)
        self.assertGreater(counts.COUNTS.sum(), 0)

    @pytest.mark.timeout(5)
    def test_redistribute_missing(self):
        df = to_df(download("se", 2010))
        df = decoders.translate_variables_SIM(df)
        variables = ["CODMUNRES", "SEXO", "IDADE_ANOS"]
        counts = SIM.group_and_count(df, variables)
        sum_original = counts["COUNTS"].sum()
        counts = SIM.redistribute_missing(counts, variables)
        sum_redistributed = counts["COUNTS"].sum()

        self.assertEqual(sum_original, sum_redistributed)

    @pytest.mark.timeout(5)
    def test_redistribute_missing_partial(self):
        df = to_df(download("se", 2010))
        df = decoders.translate_variables_SIM(
            df, age_classes=True, classify_cid10_chapters=True
        )
        group_variables = ["CODMUNRES", "SEXO", "IDADE_ANOS", "CID10_CHAPTER"]
        counts = SIM.group_and_count(df, group_variables)
        counts["COUNTS_ORIGINAL"] = counts["COUNTS"]
        sum_original = counts["COUNTS"].sum()
        counts = SIM.redistribute_missing(counts, group_variables[:3])
        sum_redistributed = counts["COUNTS"].sum()

        assert_equal(sum_original, round(sum_redistributed))
