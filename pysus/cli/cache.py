"""pysus cache — cache management commands.

Usage::

    pysus cache status   # show disk usage, file count
    pysus cache clear    # remove all cached files
"""

from __future__ import annotations

import typer

app = typer.Typer(help="Manage the PySUS cache.")


@app.command()
def status() -> None:
    """Show cache status (disk usage, file count, last modified)."""
    from pysus.api.cache_utils import cache_status, format_size

    s = cache_status()
    typer.echo(f"Cache path: {s.path}")
    typer.echo(f"Total files: {s.total_files}")
    typer.echo(f"Parquet files: {s.parquet_files}")
    typer.echo(f"Partial files: {s.partial_files}")
    typer.echo(f"Total size: {format_size(s.total_size_bytes)}")
    if s.last_modified:
        typer.echo(f"Last modified: {s.last_modified:%Y-%m-%d %H:%M:%S}")


@app.command()
def clear() -> None:
    """Remove all cached files."""
    from pysus.api.cache_utils import clear_cache

    count = clear_cache()
    typer.echo(f"Removed {count} files from cache.")
