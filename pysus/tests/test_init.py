import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from pysus import online_data


class TestInitFunctions(unittest.TestCase):
    @pytest.mark.skip(reason="This test takes too long")
    @pytest.mark.timeout(5)
    def test_last_update(self):
        for db in [
            "SINAN",
            "SIM",
            "SINASC",
            "SIH",
            "SIA",
            "PNI",
            "CNES",
            "CIHA",
        ]:
            df = online_data.FTP_Inspect(db).last_update_df()
            self.assertIsInstance(df, pd.DataFrame)
            self.assertGreater(df.size, 0)
            self.assertIn("folder", df.columns)
            self.assertIsInstance(df["date"][0], pd.Timestamp)
            self.assertEqual(df.file_size.dtype, np.dtype("int64"))


class TestListDataSources(unittest.TestCase):
    @patch("pysus.online_data.Path.exists")
    def test_list_data_sources_exists(self, mock_exists):
        dbs = "CNES, SIA, SIH, SIM, SINAN, SINASC"
        mock_exists.return_value = True
        expected_output = f"""Currently, the supported databases are: {dbs}"""
        self.assertEqual(online_data.list_data_sources(), expected_output)

    @patch("pysus.online_data.Path.exists")
    def test_list_data_sources_not_exists(self, mock_exists):
        mock_exists.return_value = False
        expected_databases = [
            "SINAN",
            "SIM",
            "SINASC",
            "SIH",
            "SIA",
            "PNI",
            "CNES",
            "CIHA",
        ]
        expected_output = f"""No support for the databases was found."
            "Expected databases for implementation are: {
                ', '.join(expected_databases)}"""
        self.assertEqual(online_data.list_data_sources(), expected_output)


if __name__ == "__main__":
    unittest.main()
