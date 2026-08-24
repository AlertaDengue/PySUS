"""Tests for pysus.cli.dadosgov CLI commands (Phase 2.2)."""

from pysus.cli import app
from typer.testing import CliRunner

runner = CliRunner()


class TestDadosGovList:
    def test_list_all_datasets(self):
        result = runner.invoke(app, ["dadosgov", "list"])
        assert result.exit_code == 0
        assert "SINAN" in result.output
        assert "Total:" in result.output


class TestDadosGovSearch:
    def test_search_exact_match(self):
        result = runner.invoke(app, ["dadosgov", "search", "SINAN"])
        assert result.exit_code == 0
        assert "SINAN" in result.output

    def test_search_no_match(self):
        result = runner.invoke(app, ["dadosgov", "search", "xyzzy"])
        assert result.exit_code == 1
        assert "No" in result.output


class TestDadosGovShow:
    def test_show_known_dataset(self):
        result = runner.invoke(app, ["dadosgov", "show", "sinan"])
        assert result.exit_code == 0
        assert "Dataset:" in result.output
        assert "SINAN" in result.output

    def test_show_unknown_dataset(self):
        result = runner.invoke(app, ["dadosgov", "show", "FAKE"])
        assert result.exit_code == 1
        assert "not found" in result.output


class TestDadosGovDownload:
    def test_download_no_token(self):
        result = runner.invoke(app, ["dadosgov", "download", "SINAN"])
        assert result.exit_code == 1
        assert "token" in result.output.lower()

    def test_download_bad_slug(self):
        import os

        os.environ["DADOSGOV_TOKEN"] = "fake_token"
        try:
            result = runner.invoke(app, ["dadosgov", "download", "FAKE"])
            assert result.exit_code == 1
            assert "not found" in result.output
        finally:
            del os.environ["DADOSGOV_TOKEN"]
