"""Configuration system for PySUS.

Supports 3-tier precedence:
    1. Explicit arguments (highest)
    2. Environment variables
    3. TOML config file (lowest)

Config file locations (first found wins):
    - ``./pysus.toml`` (project root)
    - ``~/.pysus.toml`` (home directory)

Usage::

    from pysus.config import get_config

    cfg = get_config()
    print(cfg.cache.path)          # from config file or default
    print(cfg.dadosgov.token)      # from env or config file
"""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class CacheConfig(BaseModel):
    """Cache directory settings."""

    path: str = Field(default_factory=lambda: str(Path.home() / "pysus"))


class DownloadConfig(BaseModel):
    """Download settings."""

    timeout: float = 300.0
    max_retries: int = 3
    backoff_base: float = 1.0


class DadosGovConfig(BaseModel):
    """DadosGov API settings."""

    token: str = ""


class UIConfig(BaseModel):
    """User interface settings."""

    progress_bars: bool = True


class PySUSConfig(BaseModel):
    """Root configuration for PySUS."""

    cache: CacheConfig = Field(default_factory=CacheConfig)
    download: DownloadConfig = Field(default_factory=DownloadConfig)
    dadosgov: DadosGovConfig = Field(default_factory=DadosGovConfig)
    ui: UIConfig = Field(default_factory=UIConfig)


def _find_config_file() -> Path | None:
    """Find the first existing config file."""
    project = Path("pysus.toml")
    if project.exists():
        return project

    home = Path.home() / ".pysus.toml"
    if home.exists():
        return home

    return None


def _load_toml(path: Path) -> dict[str, Any]:
    """Load a TOML file."""
    with open(path, "rb") as f:
        return tomllib.load(f)


def _apply_env_vars(data: dict[str, Any]) -> dict[str, Any]:
    """Override config values with environment variables.

    Precedence: env vars > config file defaults.
    """
    env_map: dict[str, tuple[str, str]] = {
        "PYSUS_CACHE_PATH": ("cache", "path"),
        "PYSUS_DADOSGOV_TOKEN": ("dadosgov", "token"),
        "PYSUS_DOWNLOAD_TIMEOUT": ("download", "timeout"),
        "PYSUS_DOWNLOAD_MAX_RETRIES": ("download", "max_retries"),
        "PYSUS_PROGRESS_BARS": ("ui", "progress_bars"),
    }

    for env_var, (section, key) in env_map.items():
        value = os.environ.get(env_var)
        if value is not None:
            if section not in data:
                data[section] = {}
            # Type coercion
            if key in ("timeout", "backoff_base"):
                data[section][key] = float(value)
            elif key in ("max_retries",):
                data[section][key] = int(value)
            elif key in ("progress_bars",):
                data[section][key] = value.lower() in ("1", "true", "yes")
            else:
                data[section][key] = value

    return data


def load_config(explicit: dict[str, Any] | None = None) -> PySUSConfig:
    """Load configuration with 3-tier precedence.

    Parameters
    ----------
    explicit : dict, optional
        Explicit overrides (highest precedence).

    Returns
    -------
    PySUSConfig
        Merged configuration.
    """
    data: dict[str, Any] = {}

    # 1. Load from TOML file
    config_file = _find_config_file()
    if config_file is not None:
        data = _load_toml(config_file)

    # 2. Apply environment variables
    data = _apply_env_vars(data)

    # 3. Apply explicit overrides
    if explicit:
        for section, values in explicit.items():
            if isinstance(values, dict):
                if section not in data:
                    data[section] = {}
                data[section].update(values)
            else:
                data[section] = values

    return PySUSConfig(**data)


# Module-level singleton
_config: PySUSConfig | None = None


def get_config() -> PySUSConfig:
    """Get the global configuration (lazy-loaded singleton).

    Returns
    -------
    PySUSConfig
        The current configuration.
    """
    global _config  # noqa: PLW0603
    if _config is None:
        _config = load_config()
    return _config


def reset_config() -> None:
    """Reset the configuration singleton (for testing)."""
    global _config  # noqa: PLW0603
    _config = None
