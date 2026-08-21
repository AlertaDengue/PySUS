"""Typer CLI for the DadosGov data source.

Commands
--------
pysus dadosgov list      List all DadosGov datasets
pysus dadosgov search    Search datasets
pysus dadosgov show      Show dataset details
pysus dadosgov download  Download files with optional filters
"""

from __future__ import annotations

import os

import typer

app = typer.Typer(help="DadosGov open data portal datasets")


def _get_token(
    token: str | None = None,
) -> str:
    """Resolve the DadosGov API token."""
    resolved = token or os.environ.get("DADOSGOV_TOKEN", "")
    if not resolved:
        typer.echo(
            "Error: DadosGov requires an API token.\n"
            "Get one at dados.gov.br, then:\n"
            "  export DADOSGOV_TOKEN=your_token\n"
            "  or pass --token on the command line."
        )
        raise typer.Exit(code=1)
    return resolved


@app.command("list")
def list_datasets():
    """List all available DadosGov datasets."""
    from pysus import _DADOSGOV_DESCRIPTIONS
    from pysus.api.dadosgov.databases import AVAILABLE_DATABASES

    name_w = max(len(d.__name__) for d in AVAILABLE_DATABASES)
    header = f"  {'Slug':<{name_w}}  Description"
    sep = "  " + "-" * (len(header) - 2)

    typer.echo(sep)
    typer.echo(header)
    typer.echo(sep)
    for ds_cls in AVAILABLE_DATABASES:
        name = ds_cls.__name__
        desc = _DADOSGOV_DESCRIPTIONS.get(name, "")
        typer.echo(f"  {name:<{name_w}}  {desc}")
    typer.echo(sep)
    typer.echo(f"\n  Total: {len(AVAILABLE_DATABASES)} datasets")


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query"),  # noqa: B008
):
    """Search DadosGov datasets by name or description."""
    from difflib import get_close_matches

    from pysus import _DADOSGOV_DESCRIPTIONS
    from pysus.api.dadosgov.databases import AVAILABLE_DATABASES

    query_lower = query.lower()
    matches: list[tuple[str, str]] = []

    for ds_cls in AVAILABLE_DATABASES:
        name = ds_cls.__name__
        desc = _DADOSGOV_DESCRIPTIONS.get(name, "")
        if query_lower in name.lower() or query_lower in desc.lower():
            matches.append((name, desc))

    if not matches:
        close = get_close_matches(
            query,
            [d.__name__ for d in AVAILABLE_DATABASES],
            n=3,
            cutoff=0.4,
        )
        if close:
            typer.echo(f"No exact match for '{query}'.")
            typer.echo(f"Did you mean: {', '.join(close)}?")
        else:
            typer.echo(f"No datasets match '{query}'.")
        raise typer.Exit(code=1)

    name_w = max(len(n) for n, _ in matches)
    header = f"  {'Slug':<{name_w}}  Description"
    sep = "  " + "-" * (len(header) - 2)

    typer.echo(sep)
    typer.echo(header)
    typer.echo(sep)
    for name, desc in matches:
        typer.echo(f"  {name:<{name_w}}  {desc}")
    typer.echo(sep)
    typer.echo(f"\n  Found: {len(matches)} dataset(s)")


@app.command()
def show(
    slug: str = typer.Argument(  # noqa: B008
        ..., help="Dataset slug (e.g. SINAN)"
    ),
):
    """Show details for a DadosGov dataset."""
    from pysus import _DADOSGOV_DESCRIPTIONS
    from pysus.api.dadosgov.databases import AVAILABLE_DATABASES

    slug_upper = slug.upper()
    ds_cls = None
    for d in AVAILABLE_DATABASES:
        if d.__name__.upper() == slug_upper:
            ds_cls = d
            break

    if ds_cls is None:
        typer.echo(f"Dataset '{slug}' not found.")
        typer.echo(
            "Available: " + ", ".join(d.__name__ for d in AVAILABLE_DATABASES)
        )
        raise typer.Exit(code=1)

    desc = _DADOSGOV_DESCRIPTIONS.get(ds_cls.__name__, "")
    typer.echo(f"Dataset: {ds_cls.__name__}")
    typer.echo(f"Description: {desc}")
    typer.echo("Origin: dados.gov.br (requires DADOSGOV_TOKEN)")
    typer.echo()


@app.command()
def download(
    slug: str = typer.Argument(  # noqa: B008
        ..., help="Dataset slug (e.g. SINAN)"
    ),
    group: str = typer.Option(None, help="Group/disease code"),  # noqa: B008
    state: str = typer.Option(None, help="Two-letter state"),  # noqa: B008
    year: int = typer.Option(None, help="Year to download"),  # noqa: B008
    token: str = typer.Option(  # noqa: B008
        None, "-t", "--token", help="DadosGov API token"
    ),
    output: str = typer.Option(  # noqa: B008
        None, "-o", "--output", help="Output directory"
    ),
):
    """Download files from a DadosGov dataset.

    Requires DADOSGOV_TOKEN env var or --token flag.
    """
    import pathlib

    from pysus import CACHEPATH
    from pysus.api.dadosgov.databases import AVAILABLE_DATABASES

    _get_token(token)

    slug_upper = slug.upper()
    ds_cls = None
    for d in AVAILABLE_DATABASES:
        if d.__name__.upper() == slug_upper:
            ds_cls = d
            break

    if ds_cls is None:
        typer.echo(f"Dataset '{slug}' not found.")
        raise typer.Exit(code=1)

    out_dir = pathlib.Path(output) if output else CACHEPATH / "downloads"
    out_dir.mkdir(parents=True, exist_ok=True)

    async def _do_download():
        from pysus.api.dadosgov.client import DadosGov

        dg = DadosGov()
        await dg.connect(token=token)
        try:
            datasets = await dg.datasets()
            target = None
            for ds in datasets:
                if type(ds).__name__.upper() == slug_upper:
                    target = ds
                    break

            if target is None:
                typer.echo(f"Could not initialise dataset '{slug}'.")
                raise typer.Exit(code=1)

            remote_files = target.get_files()
            if group:
                remote_files = [
                    f for f in remote_files if group.upper() in f.path.upper()
                ]
            if state:
                remote_files = [
                    f for f in remote_files if state.upper() in f.path.upper()
                ]
            if year:
                remote_files = [f for f in remote_files if str(year) in f.path]

            if not remote_files:
                typer.echo("No files match the given filters.")
                raise typer.Exit(code=0)

            typer.echo(
                f"Downloading {len(remote_files)} file(s) " f"to {out_dir}..."
            )
            for f in remote_files:
                typer.echo(f"  {f.path}")
                await dg.download(f, out_dir)

            typer.echo(f"\nDone. Files saved to {out_dir}")
        finally:
            await dg.close()

    from pysus.api.client import _run_sync

    _run_sync(_do_download())
