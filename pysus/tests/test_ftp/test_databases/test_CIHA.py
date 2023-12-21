# -*- coding:utf-8 -*-
u"""
Created on 2023/12/12
by luabida
license: GPL V3 or Later
"""
import unittest
import datetime

from unittest.mock import patch, MagicMock

from pysus.ftp.databases.ciha import CIHA
from pysus.ftp import File


class TestCIHADatabase(unittest.TestCase):

    def test_ciha(self):
        mock_content = {
            "CIHAAC1101.dbc": File(
                path="/dissemin/publicos/CIHA/201101_/Dados/CIHAAC1101.dbc",
                name="CIHAAC1101.dbc",
                info={
                    'size': 7803,
                    'type': 'file',
                    'modify': datetime.datetime(2023, 10, 6, 10, 17)
                }
            ),
            "CIHAAC1102.dbc": File(
                path="/dissemin/publicos/CIHA/201101_/Dados/CIHAAC1102.dbc",
                name="CIHAAC1102.dbc",
                info={
                    'size': 9959,
                    'type': 'file',
                    'modify': datetime.datetime(2023, 10, 6, 10, 17)
                }
            ),
            "CIHAAC1103.dbc": File(
                path="/dissemin/publicos/CIHA/201101_/Dados/CIHAAC1103.dbc",
                name="CIHAAC1103.dbc",
                info={
                    'size': 8308,
                    'type': 'file',
                    'modify': datetime.datetime(2023, 10, 6, 10, 17)
                }
            ),
        }

        with patch(
            'pysus.ftp.databases.ciha.CIHA',
            return_value=MagicMock(__content__=mock_content)
        ) as mock_ciha:
            ciha = CIHA()
            ciha.__content__ = mock_ciha().__content__

            descriptions = [ciha.describe(file) for file in ciha.files]
            expected_descriptions = [
                {'name': 'CIHAAC1101.dbc',
                 'group': 'Comunicação de Internação Hospitalar e Ambulatorial',
                 'uf': 'Acre',
                 'month': 'Janeiro',
                 'year': 2011,
                 'size': '7.8 kB',
                 'last_update': '2023-10-06 10:17AM'},
                {'name': 'CIHAAC1102.dbc',
                 'group': 'Comunicação de Internação Hospitalar e Ambulatorial',
                 'uf': 'Acre',
                 'month': 'Fevereiro',
                 'year': 2011,
                 'size': '10.0 kB',
                 'last_update': '2023-10-06 10:17AM'},
                {'name': 'CIHAAC1103.dbc',
                 'group': 'Comunicação de Internação Hospitalar e Ambulatorial',
                 'uf': 'Acre',
                 'month': 'Março',
                 'year': 2011,
                 'size': '8.3 kB',
                 'last_update': '2023-10-06 10:17AM'}
            ]

            self.assertEqual(descriptions, expected_descriptions)

            formats = [ciha.format(file) for file in ciha.files]
            expected_formats = [
                ('CIHA', 'AC', 2011, '01'),
                ('CIHA', 'AC', 2011, '02'),
                ('CIHA', 'AC', 2011, '03')
            ]
            self.assertEqual(formats, expected_formats)

            get_files = ciha.get_files(
                uf='AC', year=2011, month=1
            )
            self.assertEqual(get_files, [ciha.files[0]])
