import unittest
from pathlib import Path

import pandas as pd
from pysus.data.local import ParquetSet
from pysus.ftp import CACHE, Database, Directory, File
from pysus.ftp.databases import (
    ciha,
    cnes,
    ibge_datasus,
    pni,
    sia,
    sih,
    sim,
    sinan,
    sinasc,
)


def _test_file(testcase: unittest.TestCase, file: File):
    testcase.assertTrue(isinstance(file, File))
    testcase.assertTrue(set(["size", "type", "modify"]) == set(file.info))
    testcase.assertTrue(bool(file.basename))
    testcase.assertTrue(bool(file.name))
    testcase.assertTrue(bool(file.path))
    testcase.assertTrue(str(Path(file.path).parent) == file.parent_path)


def _test_database(testcase: unittest.TestCase, database: Database):
    testcase.assertTrue(isinstance(database, Database))
    testcase.assertTrue(bool(database.content))
    testcase.assertTrue(
        set(["description", "long_name", "source"]) == set(database.metadata)
    )

    downloaded_file = (
        database.download(database.files[0])
        if database.files[0].extension != ".zip"
        else database.download(database.files[-1])
    )
    testcase.assertTrue(isinstance(downloaded_file, ParquetSet))
    testcase.assertTrue(Path(downloaded_file.path).exists())
    testcase.assertTrue(
        isinstance(downloaded_file.to_dataframe(), pd.DataFrame)
    )
    testcase.assertTrue(not downloaded_file.to_dataframe().empty)


class TestDirectoryAndFile(unittest.TestCase):
    def setUp(self):
        self.root = Directory("/").load()

    def test_root_load(self):
        self.assertTrue(self.root.loaded)
        self.assertTrue(Directory("/dissemin") in self.root.content)

    def test_root_reload(self):
        root = self.root.reload()
        self.assertTrue(root.content == self.root.content)

    def test_root_directory(self):
        self.assertTrue(self.root.name == "/")
        self.assertTrue(self.root.path == "/")
        self.assertTrue(self.root.parent == self.root)  # outermost parent

    def test_directory_cache(self):
        self.assertTrue(CACHE["/"] == self.root)

    def test_sinan_file(self):
        file = Directory("/dissemin/publicos/SINAN/DADOS/FINAIS").content[0]
        _test_file(self, file)


class TestDatabases(unittest.TestCase):
    def test_ciha(self):
        database = ciha.CIHA().load()
        _test_database(self, database)
        self.assertTrue(database.name == "CIHA")
        self.assertSetEqual(
            set(database.describe(database.files[0])),
            {"group", "last_update", "month", "name", "size", "uf", "year"},
        )
        self.assertEqual(len(database.format(database.files[0])), 4)

    def test_cnes(self):
        database = cnes.CNES().load("DC")
        _test_database(self, database)
        self.assertTrue(database.name == "CNES")
        self.assertSetEqual(
            set(database.describe(database.files[0])),
            {"group", "last_update", "month", "name", "size", "uf", "year"},
        )
        self.assertEqual(len(database.format(database.files[0])), 4)

    def test_pni(self):
        database = pni.PNI().load()
        _test_database(self, database)
        self.assertTrue(database.name == "PNI")
        self.assertSetEqual(
            set(database.describe(database.files[0])),
            {"group", "last_update", "name", "size", "uf", "year"},
        )
        self.assertEqual(len(database.format(database.files[0])), 3)

    def test_ibge_datasus(self):
        database = ibge_datasus.IBGEDATASUS().load()
        _test_database(self, database)
        self.assertTrue(database.name == "IBGE-DataSUS")
        self.assertSetEqual(
            set(database.describe(database.files[0])),
            {"last_update", "name", "size", "year"},
        )
        self.assertEqual(len(database.format(database.files[0])), 1)

    def test_sinan(self):
        database = sinan.SINAN().load()
        _test_database(self, database)
        self.assertTrue(database.name == "SINAN")
        self.assertSetEqual(
            set(database.describe(database.files[0])),
            {"disease", "last_update", "name", "size", "year"},
        )
        self.assertEqual(len(database.format(database.files[0])), 2)

    def test_sih(self):
        database = sih.SIH().load()
        _test_database(self, database)
        self.assertTrue(database.name == "SIH")
        self.assertSetEqual(
            set(database.describe(database.files[0])),
            {"group", "last_update", "month", "name", "size", "uf", "year"},
        )
        self.assertEqual(len(database.format(database.files[0])), 4)

    def test_sinasc(self):
        database = sinasc.SINASC().load()
        _test_database(self, database)
        self.assertTrue(database.name == "SINASC")
        self.assertSetEqual(
            set(database.describe(database.files[0])),
            {"group", "last_update", "name", "size", "uf", "year"},
        )
        self.assertEqual(len(database.format(database.files[0])), 3)

    def test_sia(self):
        database = sia.SIA().load()
        _test_database(self, database)
        self.assertTrue(database.name == "SIA")
        self.assertSetEqual(
            set(database.describe(database.files[0])),
            {"group", "last_update", "month", "name", "size", "uf", "year"},
        )
        self.assertEqual(len(database.format(database.files[0])), 4)

    def test_sim(self):
        database = sim.SIM().load()
        _test_database(self, database)
        self.assertTrue(database.name == "SIM")
        self.assertSetEqual(
            set(database.describe(database.files[0])),
            {"group", "last_update", "name", "size", "uf", "year"},
        )
        self.assertEqual(len(database.format(database.files[0])), 3)
