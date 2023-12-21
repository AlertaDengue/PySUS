# -*- coding:utf-8 -*-
u"""
Created on 2023/12/12
by luabida
license: GPL V3 or Later
"""
import unittest
import datetime

from unittest.mock import patch, MagicMock

from pysus.ftp.databases.cnes import CNES
from pysus.ftp import File


class TestCNESDatabase(unittest.TestCase):

    def test_cnes(self):
        mock_content = {
            "STAC0508.dbc": File(
                path="/dissemin/publicos/CNES/200508_/Dados/ST/STAC0508.dbc",
                name="STAC0508.dbc",
                info={
                    'size': 18515,
                    'type': 'file',
                    'modify': datetime.datetime(2014, 6, 5, 9, 30)
                }
            ),
            "STAC0509.dbc": File(
                path="/dissemin/publicos/CNES/200508_/Dados/ST/STAC0509.dbc",
                name="STAC0509.dbc",
                info={
                    'size': 18713,
                    'type': 'file',
                    'modify': datetime.datetime(2014, 6, 5, 9, 30)
                }
            ),
            "STAC0510.dbc": File(
                path="/dissemin/publicos/CNES/200508_/Dados/ST/STAC0510.dbc",
                name="STAC0510.dbc",
                info={
                    'size': 17665,
                    'type': 'file',
                    'modify': datetime.datetime(2014, 6, 5, 9, 30)
                }
            ),
        }

        with patch(
            'pysus.ftp.databases.cnes.CNES',
            return_value=MagicMock(__content__=mock_content)
        ) as mock_cnes:
            cnes = CNES().load("ST")
            cnes.__content__ = mock_cnes().__content__

            descriptions = [cnes.describe(file) for file in cnes.files]
            expected_descriptions = [
                {'name': 'STAC0508.dbc',
                 'group': 'Estabelecimentos',
                 'uf': 'Acre',
                 'month': 'Agosto',
                 'year': 2005,
                 'size': '18.5 kB',
                 'last_update': '2014-06-05 09:30AM'},
                {'name': 'STAC0509.dbc',
                 'group': 'Estabelecimentos',
                 'uf': 'Acre',
                 'month': 'Setembro',
                 'year': 2005,
                 'size': '18.7 kB',
                 'last_update': '2014-06-05 09:30AM'},
                {'name': 'STAC0510.dbc',
                 'group': 'Estabelecimentos',
                 'uf': 'Acre',
                 'month': 'Outubro',
                 'year': 2005,
                 'size': '17.7 kB',
                 'last_update': '2014-06-05 09:30AM'}
            ]

            self.assertEqual(descriptions, expected_descriptions)

            formats = [cnes.format(file) for file in cnes.files]
            expected_formats = [
                ('ST', 'AC', 2005, '08'),
                ('ST', 'AC', 2005, '09'),
                ('ST', 'AC', 2005, '10')
            ]
            self.assertEqual(formats, expected_formats)

            get_files = cnes.get_files(group='ST', uf='AC', year=2005, month=8)
            self.assertEqual(get_files, [cnes.files[0]])
