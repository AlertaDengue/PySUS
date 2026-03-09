import importlib
import sys
import unittest
from unittest.mock import Mock, patch


class TestSINANOnlineDataImport(unittest.TestCase):
    MODULE = "pysus.online_data.SINAN"

    def tearDown(self):
        sys.modules.pop(self.MODULE, None)

    def test_import_does_not_connect_to_ftp(self):
        sys.modules.pop(self.MODULE, None)

        with patch(
            "pysus.ftp.databases.sinan.SINAN.load",
            side_effect=AssertionError("load() should not run on import"),
        ):
            module = importlib.import_module(self.MODULE)

        self.assertTrue(hasattr(module, "_get_sinan"))

    def test_sinan_connection_is_loaded_lazily_and_cached(self):
        sys.modules.pop(self.MODULE, None)
        fake_sinan = Mock()
        fake_sinan.diseases = {"DENG": "Dengue"}
        fake_sinan.get_files.return_value = ["f1", "f2"]
        fake_sinan.describe.side_effect = [
            {"year": "2024"},
            {"year": "2023"},
        ]

        with patch(
            "pysus.ftp.databases.sinan.SINAN.load", return_value=fake_sinan
        ) as mock_load:
            module = importlib.import_module(self.MODULE)

            self.assertEqual(module.list_diseases(), {"DENG": "Dengue"})
            self.assertEqual(module.get_available_years("DENG"), ["2023", "2024"])
            mock_load.assert_called_once()


if __name__ == "__main__":
    unittest.main()
