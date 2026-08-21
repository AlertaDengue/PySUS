"""Tests for pysus.cli.cache — cache CLI commands."""

from pysus.cli import app
from typer.testing import CliRunner

runner = CliRunner()


class TestCacheStatus:
    def test_status_shows_info(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "pysus.api.cache_utils.cache_status",
            lambda: __import__(
                "pysus.api.cache_utils", fromlist=["CacheStatus"]
            ).CacheStatus(
                path=tmp_path,
                total_files=5,
                total_size_bytes=1024,
                parquet_files=3,
                partial_files=1,
                last_modified=None,
            ),
        )
        result = runner.invoke(app, ["cache", "status"])
        assert result.exit_code == 0
        assert "5" in result.output
        assert "3" in result.output


class TestCacheClear:
    def test_clear_removes_files(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "pysus.api.cache_utils.clear_cache",
            lambda: 10,
        )
        result = runner.invoke(app, ["cache", "clear"])
        assert result.exit_code == 0
        assert "10" in result.output
