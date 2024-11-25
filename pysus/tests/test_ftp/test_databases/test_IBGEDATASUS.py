import unittest
from unittest.mock import MagicMock, patch

from pysus.ftp.databases import ibge_datasus


class IBGEDATASUSTests(unittest.TestCase):
    @patch("pysus.ftp.databases.ibge_datasus.File")
    def test_describe_zip_file(self, mock_file):
        mock_file.extension.upper.return_value = ".ZIP"
        mock_file.name = "POPTBR12.zip"
        mock_file.basename = "POPTBR12.zip"
        mock_file.info = {"size": 100, "modify": "2022-01-01"}

        ibge = ibge_datasus.IBGEDATASUS()
        result = ibge.describe(mock_file)

        self.assertEqual(
            result,
            {
                "name": "POPTBR12.zip",
                "year": 2012,
                "size": 100,
                "last_update": "2022-01-01",
            },
        )

    # @patch('pysus.ftp.databases.ibge_datasus.File')
    # def describe_dbf_file(self, mock_file):
    #     mock_file.extension.upper.return_value = ".DBF"
    #     mock_file.name = "file20.dbf"
    #     mock_file.info = {"size": 100, "modify": "2022-01-01"}
    #
    #     ibge = ibge_datasus.IBGEDATASUS()
    #     result = ibge.describe(mock_file)
    #
    #     self.assertEqual(result, {
    #         "name": "file20",
    #         "year": "2020",
    #         "size": 100,
    #         "last_update": "2022-01-01"
    #     })

    # @patch('pysus.ftp.databases.ibge_datasus.File')
    # def describe_other_file(self, mock_file):
    #     mock_file.extension.upper.return_value = ".TXT"
    #     mock_file.name = "file20.txt"
    #
    #     ibge = ibge_datasus.IBGEDATASUS()
    #     result = ibge.describe(mock_file)
    #
    #     self.assertEqual(result, {})

    @patch("pysus.ftp.databases.ibge_datasus.File")
    def format_file(self, mock_file):
        mock_file.name = "file20.zip"

        ibge = ibge_datasus.IBGEDATASUS()
        result = ibge.format(mock_file)

        self.assertEqual(result, "20.zip")

    @patch("pysus.ftp.databases.ibge_datasus.File")
    @patch("pysus.ftp.databases.ibge_datasus.to_list")
    def test_get_files_with_year(self, mock_to_list, mock_file):
        mock_file.extension.upper.return_value = ".ZIP"
        mock_file.basename = "POPTBR12.zip"
        mock_file.name = "POPTBR12"
        mock_to_list.return_value = ["2012"]

        ibge = ibge_datasus.IBGEDATASUS()
        ibge.__content__ = {"POPTBR12.zip": mock_file}
        result = ibge.get_files(year="2012")

        self.assertEqual(result[0].name, mock_file.name)

    @patch("pysus.ftp.databases.ibge_datasus.File")
    def get_files_without_year(self, mock_file):
        mock_file.extension.upper.return_value = ".ZIP"
        mock_file.name = "file20.zip"

        ibge = ibge_datasus.IBGEDATASUS()
        ibge.files = [mock_file]
        result = ibge.get_files()

        self.assertEqual(result, [mock_file])


if __name__ == "__main__":
    unittest.main()
