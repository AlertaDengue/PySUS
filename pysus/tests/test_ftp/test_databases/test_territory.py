import unittest
from pysus.ftp.databases.territory import Territory
from pysus.online_data import territory

class TestTerritory(unittest.TestCase):

    def test_load_database(self):
        T = Territory().load()

    def test_download(self):
        territory.download('todos_mapas_2013.zip')