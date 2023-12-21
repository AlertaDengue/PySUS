# -*- coding:utf-8 -*-
u"""
Created on 2023/12/12
by luabida
license: GPL V3 or Later
"""
import unittest
import datetime

from unittest.mock import patch, MagicMock

from pysus.ftp.databases.sinasc import SINASC
from pysus.ftp import File


class TestSINASCDatabase(unittest.TestCase):

    def test_sinasc(self):
        mock_content = {
            "DNAC1996.DBC": File(
                path="/dissemin/publicos/SINASC/NOV/DNRES/DNAC1996.DBC",
                name="DNAC1996.DBC",
                info={
                    'size': 247527,
                    'type': 'file',
                    'modify': datetime.datetime(2020, 1, 27, 12, 5)
                }
            ),
            "DNAC1997.DBC": File(
                path="/dissemin/publicos/SINASC/NOV/DNRES/DNAC1997.DBC",
                name="DNAC1997.DBC",
                info={
                    'size': 266815,
                    'type': 'file',
                    'modify': datetime.datetime(2020, 1, 27, 12, 5)
                }
            ),
            "DNAC1998.DBC": File(
                path="/dissemin/publicos/SINASC/NOV/DNRES/DNAC1998.DBC",
                name="DNAC1998.DBC",
                info={
                    'size': 242404,
                    'type': 'file',
                    'modify': datetime.datetime(2020, 1, 27, 12, 5)
                }
            ),
        }

        with patch(
            'pysus.ftp.databases.sinasc.SINASC',
            return_value=MagicMock(__content__=mock_content)
        ) as mock_sinasc:
            sinasc = SINASC()
            sinasc.__content__ = mock_sinasc().__content__

            descriptions = [sinasc.describe(file) for file in sinasc.files]
            expected_descriptions = [
                {'name': 'DNAC1996.DBC',
                 'group': 'Declarações de Nascidos Vivos',
                 'uf': 'Acre',
                 'year': 1996,
                 'size': '247.5 kB',
                 'last_update': '2020-01-27 12:05PM'},
                {'name': 'DNAC1997.DBC',
                 'group': 'Declarações de Nascidos Vivos',
                 'uf': 'Acre',
                 'year': 1997,
                 'size': '266.8 kB',
                 'last_update': '2020-01-27 12:05PM'},
                {'name': 'DNAC1998.DBC',
                 'group': 'Declarações de Nascidos Vivos',
                 'uf': 'Acre',
                 'year': 1998,
                 'size': '242.4 kB',
                 'last_update': '2020-01-27 12:05PM'}
            ]

            self.assertEqual(descriptions, expected_descriptions)

            formats = [sinasc.format(file) for file in sinasc.files]
            expected_formats = [
                ('DN', 'AC', 1996),
                ('DN', 'AC', 1997),
                ('DN', 'AC', 1998)
            ]
            self.assertEqual(formats, expected_formats)

            get_files = sinasc.get_files(group='DN', uf='AC', year=1996)
            self.assertEqual(get_files, [sinasc.files[0]])
