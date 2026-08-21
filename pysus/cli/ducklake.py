"""Typer CLI for the DuckLake data source.

Commands
--------
pysus ducklake list      List all DuckLake datasets
pysus ducklake search    Search datasets
pysus ducklake show      Show dataset details
pysus ducklake download  Download files
"""

from __future__ import annotations

import typer

app = typer.Typer(help="DuckLake S3-based dataset catalog")


def _get_ducklake():
    """Create and return a DuckLake client."""
    from pysus.api.ducklake.client import DuckLake

    return DuckLake()


def _list_datasets(dl) -> list:
    """Async helper to list DuckLake datasets."""
    from pysus.api.client import _run_sync

    async def _fetch():
        await dl.connect()
        try:
            return await dl.datasets()
        finally:
            await dl.close(update=False)

    return _run_sync(_fetch())


@app.command("list")
def list_datasets():
    """List all available DuckLake datasets."""
    dl = _get_ducklake()
    datasets = _list_datasets(dl)

    if not datasets:
        typer.echo("No DuckLake datasets available.")
        return

    name_w = max(len(d.name) for d in datasets)
    header = f"  {'Slug':<{name_w}}  Description"
    sep = "  " + "-" * (len(header) - 2)

    typer.echo(sep)
    typer.echo(header)
    typer.echo(sep)
    for ds in datasets:
        typer.echo(f"  {ds.name:<{name_w}}  {ds.long_name}")
    typer.echo(sep)
    typer.echo(f"\n  Total: {len(datasets)} datasets")


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query"),  # noqa: B008
):
    """Search DuckLake datasets by name."""
    dl = _get_ducklake()
    datasets = _list_datasets(dl)
    query_lower = query.lower()
    matches = [
        d
        for d in datasets
        if query_lower in d.name.lower() or query_lower in d.long_name.lower()
    ]

    if not matches:
        typer.echo(f"No DuckLake datasets match '{query}'.")
        raise typer.Exit(code=1)

    name_w = max(len(d.name) for d in matches)
    header = f"  {'Slug':<{name_w}}  Description"
    sep = "  " + "-" * (len(header) - 2)

    typer.echo(sep)
    typer.echo(header)
    typer.echo(sep)
    for ds in matches:
        typer.echo(f"  {ds.name:<{name_w}}  {ds.long_name}")
    typer.echo(sep)
    typer.echo(f"\n  Found: {len(matches)} dataset(s)")


@app.command()
def show(
    slug: str = typer.Argument(..., help="Dataset slug"),  # noqa: B008
):
    """Show details for a DuckLake dataset."""
    dl = _get_ducklake()
    datasets = _list_datasets(dl)
    target = None
    for ds in datasets:
        if ds.name.lower() == slug.lower():
            target = ds
            break

    if target is None:
        typer.echo(f"Dataset '{slug}' not found.")
        raise typer.Exit(code=1)

    typer.echo(f"Dataset: {target.name}")
    typer.echo(f"Name: {target.long_name}")
    typer.echo(f"Description: {target.description}")


@app.command()
def download(
    slug: str = typer.Argument(..., help="Dataset slug"),  # noqa: B008
    group: str = typer.Option(None, help="Group filter"),  # noqa: B008
    state: str = typer.Option(None, help="State filter"),  # noqa: B008
    year: int = typer.Option(None, help="Year filter"),  # noqa: B008
    output: str = typer.Option(  # noqa: B008
        None, "-o", "--output", help="Output directory"
    ),
):
    """Download files from a DuckLake dataset."""
    import pathlib

    from pysus import CACHEPATH
    from pysus.api.client import _run_sync

    out_dir = pathlib.Path(output) if output else CACHEPATH / "downloads"
    out_dir.mkdir(parents=True, exist_ok=True)

    dl = _get_ducklake()
    datasets = _list_datasets(dl)
    target = None
    for ds in datasets:
        if ds.name.lower() == slug.lower():
            target = ds
            break

    if target is None:
        typer.echo(f"Dataset '{slug}' not found.")
        raise typer.Exit(code=1)

    async def _do_download():
        files = await target.query(group=group, state=state, year=year)
        if not files:
            typer.echo("No files match the given filters.")
            raise typer.Exit(code=0)

        typer.echo(f"Downloading {len(files)} file(s) to {out_dir}...")
        for f in files:
            typer.echo(f"  {f.path}")
            await target.download(f, out_dir)

        typer.echo(f"\nDone. Files saved to {out_dir}")

    _run_sync(_do_download())
