# -*- coding:utf-8 -*-
u"""
Created on 2023/12/11
by luabida
license: GPL V3 or Later
"""
import unittest
from datetime import datetime

from unittest.mock import patch, MagicMock

from pysus.ftp.databases.sim import SIM
from pysus.ftp import File


class TestSIMDatabase(unittest.TestCase):

    def test_sim(self):
        date_format = '%Y-%m-%d %I:%M%p'

        mock_content = {
            "DOAC1996.dbc": File(
                path="/dissemin/publicos/SIM/CID10/DORES/DOAC1996.dbc",
                name="DOAC1996.dbc",
                info={
                    'size': 78054.4,
                    'type': 'DBC file',
                    'modify': datetime.strptime('2020-01-31 02:48PM', date_format)
                }
            ),
            "DOAC1997.dbc": File(
                path="/dissemin/publicos/SIM/CID10/DORES/DOAC1997.dbc",
                name="DOAC1997.dbc",
                info={
                    'size': 79084.8,
                    'type': 'DBC file',
                    'modify': datetime.strptime('2020-01-31 02:48PM', date_format)
                }
            ),
            "DOAC1998.dbc": File(
                path="/dissemin/publicos/SIM/CID10/DORES/DOAC1998.dbc",
                name="DOAC1998.dbc",
                info={
                    'size': 79084.8,
                    'type': 'DBC file',
                    'modify': datetime.strptime('2020-01-31 02:48PM', date_format)
                }
            ),
        }

        with patch(
                'pysus.ftp.databases.sim.SIM',
                return_value=MagicMock(__content__=mock_content)
        ) as mock_sim:
            sim = SIM()
            sim.__content__ = mock_sim().__content__

            descriptions = [sim.describe(file) for file in sim.files]
            expected_descriptions = [
                {'name': 'DOAC1996.dbc',
                 'uf': 'Acre',
                 'year': 1996,
                 'group': 'CID10',
                 'size': '78.1 kB',
                 'last_update': '2020-01-31 02:48PM'},
                {'name': 'DOAC1997.dbc',
                 'uf': 'Acre',
                 'year': 1997,
                 'group': 'CID10',
                 'size': '79.1 kB',
                 'last_update': '2020-01-31 02:48PM'},
                {'name': 'DOAC1998.dbc',
                 'uf': 'Acre',
                 'year': 1998,
                 'group': 'CID10',
                 'size': '79.1 kB',
                 'last_update': '2020-01-31 02:48PM'}
            ]

            self.assertEqual(descriptions, expected_descriptions)

            formats = [sim.format(file) for file in sim.files]
            expected_formats = [
                ('DO', 'AC', 1996), ('DO', 'AC', 1997), ('DO', 'AC', 1998)
            ]
            self.assertEqual(formats, expected_formats)

            get_files = sim.get_files(
                group='CID10', uf='AC', year='1996'
            )
            self.assertEqual(get_files, [sim.files[0]])
