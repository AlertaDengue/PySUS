"""Tests for pysus.cli.ducklake CLI commands (Phase 2.3)."""

from unittest.mock import MagicMock, patch

from pysus.cli import app
from typer.testing import CliRunner

runner = CliRunner()


def _make_mock_dataset(name="dengue", long_name="Dengue dataset"):
    ds = MagicMock()
    ds.name = name
    ds.long_name = long_name
    ds.description = "Test dataset"
    ds.path = f"/data/{name}/file.parquet"
    return ds


class TestDuckLakeList:
    @patch("pysus.cli.ducklake._list_datasets")
    def test_list_all_datasets(self, mock_list):
        mock_list.return_value = [
            _make_mock_dataset("dengue", "Dengue data"),
            _make_mock_dataset("malaria", "Malaria data"),
        ]
        result = runner.invoke(app, ["ducklake", "list"])
        assert result.exit_code == 0
        assert "dengue" in result.output
        assert "Total:" in result.output


class TestDuckLakeSearch:
    @patch("pysus.cli.ducklake._list_datasets")
    def test_search_match(self, mock_list):
        mock_list.return_value = [
            _make_mock_dataset("dengue", "Dengue data"),
        ]
        result = runner.invoke(app, ["ducklake", "search", "dengue"])
        assert result.exit_code == 0
        assert "dengue" in result.output

    @patch("pysus.cli.ducklake._list_datasets")
    def test_search_no_match(self, mock_list):
        mock_list.return_value = [
            _make_mock_dataset("dengue", "Dengue data"),
        ]
        result = runner.invoke(app, ["ducklake", "search", "xyzzy"])
        assert result.exit_code == 1


class TestDuckLakeShow:
    @patch("pysus.cli.ducklake._list_datasets")
    def test_show_known(self, mock_list):
        mock_list.return_value = [
            _make_mock_dataset("dengue", "Dengue data"),
        ]
        result = runner.invoke(app, ["ducklake", "show", "dengue"])
        assert result.exit_code == 0
        assert "Dataset: dengue" in result.output

    @patch("pysus.cli.ducklake._list_datasets")
    def test_show_unknown(self, mock_list):
        mock_list.return_value = []
        result = runner.invoke(app, ["ducklake", "show", "FAKE"])
        assert result.exit_code == 1
        assert "not found" in result.output
