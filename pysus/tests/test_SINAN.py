# -*- coding:utf-8 -*-
u"""
Created on 2023/12/12
by luabida
license: GPL V3 or Later
"""
import unittest
import datetime

from unittest.mock import patch, MagicMock

from pysus.ftp.databases.sinan import SINAN
from pysus.ftp import File


class TestSINANDatabase(unittest.TestCase):

    def test_sinan(self):
        mock_content = {
            "ACBIBR06.dbc": File(
                path="/dissemin/publicos/SINAN/DADOS/FINAIS/ACBIBR06.dbc",
                name="ACBIBR06.dbc",
                info={
                    'size': 28326,
                    'type': 'file',
                    'modify': datetime.datetime(2023, 1, 16, 14, 15)
                }
            ),
            "ACBIBR07.dbc": File(
                path="/dissemin/publicos/SINAN/DADOS/FINAIS/ACBIBR07.dbc",
                name="ACBIBR07.dbc",
                info={
                    'size': 673314,
                    'type': 'file',
                    'modify': datetime.datetime(2023, 1, 16, 14, 15)
                }
            ),
            "ACBIBR08.dbc": File(
                path="/dissemin/publicos/SINAN/DADOS/FINAIS/ACBIBR08.dbc",
                name="ACBIBR08.dbc",
                info={
                    'size': 1048406,
                    'type': 'file',
                    'modify': datetime.datetime(2023, 1, 16, 14, 15)
                }
            ),
        }

        with patch(
            'pysus.ftp.databases.sinan.SINAN',
            return_value=MagicMock(__content__=mock_content)
        ) as mock_sinan:
            sinan = SINAN()
            sinan.__content__ = mock_sinan().__content__

            descriptions = [sinan.describe(file) for file in sinan.files]
            expected_descriptions = [
                {'name': 'ACBIBR06.dbc',
                 'disease': 'Acidente de trabalho com material biológico',
                 'year': 2006,
                 'size': '28.3 kB',
                 'last_update': '2023-01-16 02:15PM'},
                {'name': 'ACBIBR07.dbc',
                 'disease': 'Acidente de trabalho com material biológico',
                 'year': 2007,
                 'size': '673.3 kB',
                 'last_update': '2023-01-16 02:15PM'},
                {'name': 'ACBIBR08.dbc',
                 'disease': 'Acidente de trabalho com material biológico',
                 'year': 2008,
                 'size': '1.0 MB',
                 'last_update': '2023-01-16 02:15PM'}
            ]

            self.assertEqual(descriptions, expected_descriptions)

            formats = [sinan.format(file) for file in sinan.files]
            expected_formats = [('ACBI', 2006), ('ACBI', 2007), ('ACBI', 2008)]
            self.assertEqual(formats, expected_formats)

            get_files = sinan.get_files(dis_code='ACBI', year=2006)
            self.assertEqual(get_files, [sinan.files[0]])
