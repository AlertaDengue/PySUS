# -*- coding:utf-8 -*-
u"""
Created on 2023/12/11
by luabida
license: GPL V3 or Later
"""
import unittest
import datetime

from unittest.mock import patch, MagicMock

from pysus.ftp.databases.sia import SIA
from pysus.ftp import File


class TestSIADatabase(unittest.TestCase):

    def test_sia(self):
        mock_content = {
            "ABDF1112.dbc": File(
                path="/dissemin/publicos/SIASUS/200801_/Dados/ABDF1112.dbc",
                name="ABDF1112.dbc",
                info={
                    'size': 2971,
                    'type': 'file',
                    'modify': datetime.datetime(2019, 3, 12, 12, 3)
                }
            ),
            "ABMG1112.dbc": File(
                path="/dissemin/publicos/SIASUS/200801_/Dados/ABMG1112.dbc",
                name="ABMG1112.dbc",
                info={
                    'size': 3183,
                    'type': 'file',
                    'modify': datetime.datetime(2019, 3, 12, 12, 3)
                }
            ),
            "ABOAC1502.dbc": File(
                path="/dissemin/publicos/SIASUS/200801_/Dados/ABOAC1502.dbc",
                name="ABOAC1502.dbc",
                info={
                    'size': 3143,
                    'type': 'file',
                    'modify': datetime.datetime(2016, 9, 12, 8, 45)
                }
            ),
        }

        with patch(
            'pysus.ftp.databases.sia.SIA',
            return_value=MagicMock(__content__=mock_content)
        ) as mock_sia:
            sia = SIA()
            sia.__content__ = mock_sia().__content__

            descriptions = [sia.describe(file) for file in sia.files]
            expected_descriptions = [
                {'name': 'ABDF1112.dbc',
                 'group': 'APAC de Cirurgia Bariátrica',
                 'uf': 'Distrito Federal',
                 'month': 'Dezembro',
                 'year': 2011,
                 'size': '3.0 kB',
                 'last_update': '2019-03-12 12:03PM'},
                {'name': 'ABMG1112.dbc',
                 'group': 'APAC de Cirurgia Bariátrica',
                 'uf': 'Minas Gerais',
                 'month': 'Dezembro',
                 'year': 2011,
                 'size': '3.2 kB',
                 'last_update': '2019-03-12 12:03PM'},
                {'name': 'ABOAC1502.dbc',
                 'group': 'APAC de Acompanhamento Pós Cirurgia Bariátrica',
                 'uf': 'Acre',
                 'month': 'Fevereiro',
                 'year': 2015,
                 'size': '3.1 kB',
                 'last_update': '2016-09-12 08:45AM'}
            ]

            self.assertEqual(descriptions, expected_descriptions)

            formats = [sia.format(file) for file in sia.files]
            expected_formats = [
                ('AB', 'DF', 2011, '12'),
                ('AB', 'MG', 2011, '12'),
                ('ABO', 'AC', 2015, '02')
            ]
            self.assertEqual(formats, expected_formats)

            get_files = sia.get_files(
                group='AB', uf='DF', year=2011, month=12
            )
            self.assertEqual(get_files, [sia.files[0]])
