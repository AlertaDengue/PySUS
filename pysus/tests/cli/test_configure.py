"""Tests for pysus.cli.configure — configuration CLI."""

from pysus.cli import app
from pysus.config import reset_config
from typer.testing import CliRunner

runner = CliRunner()


class TestConfigure:
    def test_non_interactive_creates_toml(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("PYSUS_CACHE_PATH", raising=False)
        reset_config()

        result = runner.invoke(
            app,
            ["configure", "--non-interactive"],
        )
        assert result.exit_code == 0
        assert "saved" in result.output.lower()
        assert (tmp_path / "pysus.toml").exists()

    def test_non_interactive_with_flags(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("PYSUS_CACHE_PATH", raising=False)
        reset_config()

        result = runner.invoke(
            app,
            [
                "configure",
                "--non-interactive",
                "--cache-path",
                "/custom/cache",
                "--token",
                "test_token",
                "--timeout",
                "120",
            ],
        )
        assert result.exit_code == 0
        content = (tmp_path / "pysus.toml").read_text()
        assert "/custom/cache" in content
        assert "test_token" in content
        assert "120" in content

    def test_toml_content_valid(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("PYSUS_CACHE_PATH", raising=False)
        reset_config()

        runner.invoke(app, ["configure", "--non-interactive"])
        content = (tmp_path / "pysus.toml").read_text()
        assert "[cache]" in content
        assert "[download]" in content
        assert "[dadosgov]" in content
        assert "[ui]" in content

    def test_toml_is_loadable(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("PYSUS_CACHE_PATH", raising=False)
        reset_config()

        runner.invoke(app, ["configure", "--non-interactive"])

        import tomllib

        data = tomllib.loads((tmp_path / "pysus.toml").read_text())
        assert "cache" in data
        assert "download" in data
