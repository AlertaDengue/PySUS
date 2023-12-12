# -*- coding:utf-8 -*-
u"""
Created on 2023/12/11
by luabida
license: GPL V3 or Later
"""
import unittest
import datetime

from unittest.mock import patch, MagicMock

from pysus.ftp.databases.sih import SIH
from pysus.ftp import File


class TestSIHDatabase(unittest.TestCase):

    def test_sih(self):
        mock_content = {
            "CHBR1901.dbc": File(
                path="/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1901.dbc",
                name="CHBR1901.dbc",
                info={
                    'size': 196476,
                    'type': 'file',
                    'modify': datetime.datetime(2020, 3, 10, 14, 43)
                }
            ),
            "CHBR1902.dbc": File(
                path="/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1902.dbc",
                name="CHBR1902.dbc",
                info={
                    'size': 196287,
                    'type': 'file',
                    'modify': datetime.datetime(2020, 3, 10, 14, 43)
                }
            ),
            "CHBR1903.dbc": File(
                path="/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1903.dbc",
                name="CHBR1903.dbc",
                info={
                    'size': 196081,
                    'type': 'file',
                    'modify': datetime.datetime(2020, 3, 10, 14, 43)
                }
            ),
        }

        with patch(
            'pysus.ftp.databases.sih.SIH',
            return_value=MagicMock(__content__=mock_content)
        ) as mock_sih:
            sih = SIH()
            sih.__content__ = mock_sih().__content__

            descriptions = [sih.describe(file) for file in sih.files]
            expected_descriptions = [
                {'name': 'CHBR1901.dbc',
                 'group': 'Cadastro Hospitalar',
                 'uf': 'Brasil',
                 'month': 'Janeiro',
                 'year': 2019,
                 'size': '196.5 kB',
                 'last_update': '2020-03-10 02:43PM'},
                {'name': 'CHBR1902.dbc',
                 'group': 'Cadastro Hospitalar',
                 'uf': 'Brasil',
                 'month': 'Fevereiro',
                 'year': 2019,
                 'size': '196.3 kB',
                 'last_update': '2020-03-10 02:43PM'},
                {'name': 'CHBR1903.dbc',
                 'group': 'Cadastro Hospitalar',
                 'uf': 'Brasil',
                 'month': 'Março',
                 'year': 2019,
                 'size': '196.1 kB',
                 'last_update': '2020-03-10 02:43PM'}
            ]

            self.assertEqual(descriptions, expected_descriptions)

            formats = [sih.format(file) for file in sih.files]
            expected_formats = [
                ('CH', 'BR', 2019, '01'),
                ('CH', 'BR', 2019, '02'),
                ('CH', 'BR', 2019, '03')
            ]
            self.assertEqual(formats, expected_formats)

            get_files = sih.get_files(
                group='CH', uf='BR', year=2019, month=1
            )
            self.assertEqual(get_files, [sih.files[0]])
