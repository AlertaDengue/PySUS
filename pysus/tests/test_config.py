"""Tests for pysus.config — configuration system."""

from pathlib import Path

from pysus.config import (
    PySUSConfig,
    _apply_env_vars,
    _find_config_file,
    _load_toml,
    get_config,
    load_config,
    reset_config,
)


class TestFindConfigFile:
    def test_returns_none_when_no_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(Path, "home", lambda: tmp_path / "nonexistent")
        assert _find_config_file() is None

    def test_finds_project_root(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "pysus.toml").write_text("[cache]\npath = '/tmp'\n")
        result = _find_config_file()
        assert result == Path("pysus.toml")

    def test_finds_home_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        home_dir = tmp_path / "home"
        home_dir.mkdir()
        (home_dir / ".pysus.toml").write_text("[cache]\npath = '/tmp'\n")
        monkeypatch.setattr(Path, "home", lambda: home_dir)
        result = _find_config_file()
        assert result == home_dir / ".pysus.toml"

    def test_project_overrides_home(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "pysus.toml").write_text("[cache]\npath = '/proj'\n")
        home_dir = tmp_path / "home"
        home_dir.mkdir()
        (home_dir / ".pysus.toml").write_text("[cache]\npath = '/home'\n")
        monkeypatch.setattr(Path, "home", lambda: home_dir)
        result = _find_config_file()
        assert result == Path("pysus.toml")


class TestLoadToml:
    def test_loads_valid_toml(self, tmp_path):
        toml_file = tmp_path / "test.toml"
        toml_file.write_text(
            "[cache]\npath = '/data'\n\n[download]\ntimeout = 600\n"
        )
        data = _load_toml(toml_file)
        assert data["cache"]["path"] == "/data"
        assert data["download"]["timeout"] == 600


class TestApplyEnvVars:
    def test_cache_path(self, monkeypatch):
        monkeypatch.setenv("PYSUS_CACHE_PATH", "/env/cache")
        data = _apply_env_vars({})
        assert data["cache"]["path"] == "/env/cache"

    def test_dadosgov_token(self, monkeypatch):
        monkeypatch.setenv("PYSUS_DADOSGOV_TOKEN", "tok_123")
        data = _apply_env_vars({})
        assert data["dadosgov"]["token"] == "tok_123"

    def test_download_timeout(self, monkeypatch):
        monkeypatch.setenv("PYSUS_DOWNLOAD_TIMEOUT", "120.5")
        data = _apply_env_vars({})
        assert data["download"]["timeout"] == 120.5

    def test_progress_bars(self, monkeypatch):
        monkeypatch.setenv("PYSUS_PROGRESS_BARS", "false")
        data = _apply_env_vars({})
        assert data["ui"]["progress_bars"] is False

    def test_env_overrides_toml(self, monkeypatch):
        monkeypatch.setenv("PYSUS_CACHE_PATH", "/env/path")
        data = _apply_env_vars({"cache": {"path": "/toml/path"}})
        assert data["cache"]["path"] == "/env/path"


class TestLoadConfig:
    def test_defaults(self):
        config = load_config()
        assert isinstance(config, PySUSConfig)
        assert config.download.timeout == 300.0
        assert config.download.max_retries == 3

    def test_explicit_overrides(self):
        config = load_config(explicit={"download": {"timeout": 999.0}})
        assert config.download.timeout == 999.0

    def test_env_overrides_defaults(self, monkeypatch):
        monkeypatch.setenv("PYSUS_CACHE_PATH", "/env/test")
        reset_config()
        config = load_config()
        assert config.cache.path == "/env/test"
        monkeypatch.delenv("PYSUS_CACHE_PATH")
        reset_config()


class TestGetConfig:
    def test_returns_singleton(self):
        reset_config()
        c1 = get_config()
        c2 = get_config()
        assert c1 is c2

    def test_reset_config(self):
        reset_config()
        c1 = get_config()
        reset_config()
        c2 = get_config()
        assert c1 is not c2
