"""pysus configure — interactive configuration setup.

Usage::

    pysus configure                 # interactive
    pysus configure --non-interactive  # env vars only
"""

from __future__ import annotations

from pathlib import Path

import typer
from pysus.config import PySUSConfig

app = typer.Typer(help="Configure PySUS settings.")


@app.callback(invoke_without_command=True)
def configure(
    ctx: typer.Context,
    non_interactive: bool = typer.Option(  # noqa: B008
        False,
        "--non-interactive",
        help="Skip prompts, use env vars and defaults only.",
    ),
    cache_path: str | None = typer.Option(  # noqa: B008
        None,
        "--cache-path",
        help="Set cache directory path.",
    ),
    token: str | None = typer.Option(  # noqa: B008
        None,
        "--token",
        help="Set DadosGov API token.",
    ),
    timeout: float | None = typer.Option(  # noqa: B008
        None,
        "--timeout",
        help="Set download timeout in seconds.",
    ),
) -> None:
    """Configure PySUS settings interactively or via flags."""
    from pysus.config import get_config

    config = get_config()

    if non_interactive:
        _apply_flags(config, cache_path, token, timeout)
        _save_config(config)
        typer.echo("Configuration saved.")
        return

    typer.echo("PySUS Configuration")
    typer.echo("=" * 40)

    new_cache = typer.prompt(
        "Cache directory",
        default=config.cache.path,
    )
    config.cache.path = new_cache

    new_token = typer.prompt(
        "DadosGov API token (leave empty to skip)",
        default=config.dadosgov.token or "",
        show_default=False,
    )
    if new_token:
        config.dadosgov.token = new_token

    new_timeout = typer.prompt(
        "Download timeout (seconds)",
        default=str(config.download.timeout),
    )
    config.download.timeout = float(new_timeout)

    progress = typer.confirm(
        "Show progress bars?",
        default=config.ui.progress_bars,
    )
    config.ui.progress_bars = progress

    _save_config(config)
    typer.echo(f"\nConfiguration saved to {_config_path()}")


def _apply_flags(
    config: PySUSConfig,
    cache_path: str | None,
    token: str | None,
    timeout: float | None,
) -> None:
    """Apply CLI flags to config."""
    if cache_path is not None:
        config.cache.path = cache_path
    if token is not None:
        config.dadosgov.token = token
    if timeout is not None:
        config.download.timeout = timeout


def _config_path() -> Path:
    """Get the config file path (project root)."""
    return Path("pysus.toml")


def _save_config(config: PySUSConfig) -> None:
    """Save config to TOML file."""
    path = _config_path()
    lines = ["# PySUS configuration file", ""]

    lines.append("[cache]")
    lines.append(f'path = "{str(config.cache.path).replace(chr(92), "/")}"')
    lines.append("")

    lines.append("[download]")
    lines.append(f"timeout = {config.download.timeout}")
    lines.append(f"max_retries = {config.download.max_retries}")
    lines.append("")

    lines.append("[dadosgov]")
    token = config.dadosgov.token
    lines.append(f'token = "{token}"')
    lines.append("")

    lines.append("[ui]")
    progress = str(config.ui.progress_bars).lower()
    lines.append(f"progress_bars = {progress}")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
