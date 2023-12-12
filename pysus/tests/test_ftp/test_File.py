# -*- coding:utf-8 -*-
u"""
Created on 2023/12/12
by luabida
license: GPL V3 or Later
"""
import unittest
import datetime
from pathlib import Path

import pandas as pd

from pysus.ftp import File, CACHEPATH
from pysus.data.local import ParquetSet


class TestFile(unittest.TestCase):

    def setUp(self):
        path = "/dissemin/publicos/SIM/CID10/DORES/"
        name = "DOAC1996.dbc"
        info = {
            "size": 76107,
            "type": "file",
            "modify": datetime.datetime(2020, 1, 31, 14, 48)
        }

        self.file = File(path, name, info)

    def test_file_initialization(self):
        file = self.file

        expected_path = "/dissemin/publicos/SIM/CID10/DORES/DOAC1996.dbc"
        self.assertEqual(file.path, expected_path)

        self.assertEqual(file.name, "DOAC1996")

        self.assertEqual(file.extension, ".dbc")

        self.assertEqual(file.basename, "DOAC1996.dbc")

        expected_info = {
            'size': '76.1 kB',
            'type': 'DBC file',
            'modify': '2020-01-31 02:48PM'
        }
        self.assertEqual(file.info, expected_info)

    def test_file_download(self):
        parquet = self.file.download()

        self.assertIsInstance(parquet, ParquetSet)

        self.assertTrue("size" in parquet.info)

        local_cache = Path(CACHEPATH)
        expected_local_path = local_cache / "DOAC1996.parquet"
        self.assertTrue(expected_local_path.exists())
        self.assertEqual(Path(str(parquet)), expected_local_path)

        df = parquet.to_dataframe()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
