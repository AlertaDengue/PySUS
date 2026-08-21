"""Tests for pysus CLI global commands (Phase 2.4)."""

from pysus.cli import app
from typer.testing import CliRunner

runner = CliRunner()


class TestGlobalSearch:
    def test_search_ftp_match(self):
        result = runner.invoke(app, ["search", "SINAN"])
        assert result.exit_code == 0
        assert "SINAN" in result.output
        assert "FTP" in result.output
        assert "Found:" in result.output

    def test_search_multi_origin(self):
        result = runner.invoke(app, ["search", "SIM"])
        assert result.exit_code == 0
        # SIM should exist in FTP at least
        assert "SIM" in result.output

    def test_search_no_match(self):
        result = runner.invoke(app, ["search", "xyzzy"])
        assert result.exit_code == 1
        assert "No" in result.output


class TestGlobalInfo:
    def test_info_shows_datasets(self):
        result = runner.invoke(app, ["info"])
        assert result.exit_code == 0
        assert "Total:" in result.output


class TestGlobalVersion:
    def test_version(self):
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0
        assert "2." in result.output
