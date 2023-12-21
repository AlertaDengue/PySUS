# -*- coding:utf-8 -*-
u"""
Created on 2023/12/12
by luabida
license: GPL V3 or Later
"""
import unittest
import datetime

from unittest.mock import patch, MagicMock

from pysus.ftp.databases.pni import PNI
from pysus.ftp import File


class TestPNIDatabase(unittest.TestCase):

    def test_pni(self):
        mock_content = {
            "CPNIAC00.DBF": File(
                path="/dissemin/publicos/PNI/DADOS/CPNIAC00.DBF",
                name="CPNIAC00.DBF",
                info={
                    'size': 14843,
                    'type': 'file',
                    'modify': datetime.datetime(2019, 5, 23, 17, 19)
                }
            ),
            "CPNIAC01.DBF": File(
                path="/dissemin/publicos/PNI/DADOS/CPNIAC01.DBF",
                name="CPNIAC01.DBF",
                info={
                    'size': 14843,
                    'type': 'file',
                    'modify': datetime.datetime(2019, 5, 23, 16, 39)
                }
            ),
            "CPNIAC02.DBF": File(
                path="/dissemin/publicos/PNI/DADOS/CPNIAC02.DBF",
                name="CPNIAC02.DBF",
                info={
                    'size': 14843,
                    'type': 'file',
                    'modify': datetime.datetime(2019, 5, 23, 16, 39)
                }
            ),
        }

        with patch(
            'pysus.ftp.databases.pni.PNI',
            return_value=MagicMock(__content__=mock_content)
        ) as mock_pni:
            pni = PNI()
            pni.__content__ = mock_pni().__content__

            descriptions = [pni.describe(file) for file in pni.files]
            expected_descriptions = [
                {'name': 'CPNIAC00.DBF',
                 'group': 'Cobertura Vacinal',
                 'uf': 'Acre',
                 'year': 2000,
                 'size': '14.8 kB',
                 'last_update': '2019-05-23 05:19PM'},
                {'name': 'CPNIAC01.DBF',
                 'group': 'Cobertura Vacinal',
                 'uf': 'Acre',
                 'year': 2001,
                 'size': '14.8 kB',
                 'last_update': '2019-05-23 04:39PM'},
                {'name': 'CPNIAC02.DBF',
                 'group': 'Cobertura Vacinal',
                 'uf': 'Acre',
                 'year': 2002,
                 'size': '14.8 kB',
                 'last_update': '2019-05-23 04:39PM'}
            ]
            self.assertEqual(descriptions, expected_descriptions)

            formats = [pni.format(file) for file in pni.files]
            expected_formats = [
                ('CPNI', 'AC', 2000),
                ('CPNI', 'AC', 2001),
                ('CPNI', 'AC', 2002)
            ]
            self.assertEqual(formats, expected_formats)

            get_files = pni.get_files(group='CPNI', uf='AC', year=2000)
            self.assertEqual(get_files, [pni.files[0]])
