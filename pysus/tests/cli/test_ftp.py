"""Tests for pysus.cli.ftp CLI commands (Phase 2.1)."""

from pysus.cli import app
from typer.testing import CliRunner

runner = CliRunner()


class TestFTPList:
    def test_list_all_datasets(self):
        result = runner.invoke(app, ["ftp", "list"])
        assert result.exit_code == 0
        assert "SINAN" in result.output
        assert "SIH" in result.output
        assert "Total:" in result.output


class TestFTPSearch:
    def test_search_exact_match(self):
        result = runner.invoke(app, ["ftp", "search", "SINAN"])
        assert result.exit_code == 0
        assert "SINAN" in result.output

    def test_search_fuzzy(self):
        result = runner.invoke(app, ["ftp", "search", "hospital"])
        assert result.exit_code == 0
        assert "SIH" in result.output or "CIHA" in result.output

    def test_search_no_match(self):
        result = runner.invoke(app, ["ftp", "search", "xyzzy"])
        assert result.exit_code == 1
        assert "No" in result.output


class TestFTPShow:
    def test_show_known_dataset(self):
        result = runner.invoke(app, ["ftp", "show", "sinan"])
        assert result.exit_code == 0
        assert "Dataset:" in result.output
        assert "SINAN" in result.output

    def test_show_unknown_dataset(self):
        result = runner.invoke(app, ["ftp", "show", "FAKE"])
        assert result.exit_code == 1
        assert "not found" in result.output


class TestFTPDownload:
    def test_download_bad_slug(self):
        result = runner.invoke(app, ["ftp", "download", "FAKE"])
        assert result.exit_code == 1
        assert "not found" in result.output
