"""Typer CLI for the FTP data source.

Commands
--------
pysus ftp list          List all available FTP datasets
pysus ftp search <q>    Search datasets by name or description
pysus ftp show <slug>   Show dataset details (groups, formats, years)
pysus ftp download <slug> Download files with optional filters
"""

from __future__ import annotations

import typer

app = typer.Typer(help="DATASUS FTP datasets")


def _get_ftp():
    """Create and return an FTP client."""
    from pysus.api.ftp.client import FTP

    return FTP()


@app.command("list")
def list_datasets():
    """List all available FTP datasets."""
    from pysus.api._impl._ui import _FTP_DESC as _FTP_DESCRIPTIONS
    from pysus.api.ftp.databases import AVAILABLE_DATABASES

    name_w = max(len(d.__name__) for d in AVAILABLE_DATABASES)
    header = f"  {'Slug':<{name_w}}  Description"
    sep = "  " + "-" * (len(header) - 2)

    typer.echo(sep)
    typer.echo(header)
    typer.echo(sep)
    for ds_cls in AVAILABLE_DATABASES:
        name = ds_cls.__name__
        desc = _FTP_DESCRIPTIONS.get(name, "")
        typer.echo(f"  {name:<{name_w}}  {desc}")
    typer.echo(sep)
    typer.echo(f"\n  Total: {len(AVAILABLE_DATABASES)} datasets")


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query"),  # noqa: B008
):
    """Search datasets by name or description."""
    from difflib import get_close_matches

    from pysus.api._impl._ui import _FTP_DESC as _FTP_DESCRIPTIONS
    from pysus.api.ftp.databases import AVAILABLE_DATABASES

    query_lower = query.lower()
    matches: list[tuple[str, str]] = []

    for ds_cls in AVAILABLE_DATABASES:
        name = ds_cls.__name__
        desc = _FTP_DESCRIPTIONS.get(name, "")
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
    """Show details for a dataset: groups, files, formats, years."""
    from pysus.api._impl._ui import _FTP_DESC as _FTP_DESCRIPTIONS
    from pysus.api.ftp.databases import AVAILABLE_DATABASES

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

    desc = _FTP_DESCRIPTIONS.get(ds_cls.__name__, "")
    typer.echo(f"Dataset: {ds_cls.__name__}")
    typer.echo(f"Description: {desc}")
    typer.echo()

    if hasattr(ds_cls, "group_definitions") and ds_cls.group_definitions:
        typer.echo("Groups:")
        for code, name in ds_cls.group_definitions.items():
            typer.echo(f"  {code:<8s} {name}")
        typer.echo()

    if hasattr(ds_cls, "paths") and ds_cls.paths:
        typer.echo("FTP paths:")
        for p in ds_cls.paths:
            typer.echo(f"  {p.path if hasattr(p, 'path') else p}")
        typer.echo()

    typer.echo(
        "To list available files, run: "
        f"pysus ftp files {ds_cls.__name__.lower()}"
    )


@app.command()
def files(
    slug: str = typer.Argument(  # noqa: B008
        ..., help="Dataset slug (e.g. SINAN)"
    ),
    group: str = typer.Option(None, help="Group/disease code"),  # noqa: B008
    state: str = typer.Option(None, help="Two-letter state"),  # noqa: B008
    year: int = typer.Option(None, help="Year to list"),  # noqa: B008
):
    """List available files for a dataset with optional filters."""
    from pysus.api.ftp.databases import AVAILABLE_DATABASES

    slug_upper = slug.upper()
    ds_cls = None
    for d in AVAILABLE_DATABASES:
        if d.__name__.upper() == slug_upper:
            ds_cls = d
            break

    if ds_cls is None:
        typer.echo(f"Dataset '{slug}' not found.")
        raise typer.Exit(code=1)

    ftp = _get_ftp()
    try:
        datasets = ftp.datasets()
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

        typer.echo(f"  Files for {ds_cls.__name__}: " f"{len(remote_files)}\n")
        for f in remote_files[:50]:
            typer.echo(f"  {f.path}")
        if len(remote_files) > 50:
            typer.echo(f"  ... and {len(remote_files) - 50} more")
    finally:
        ftp.close()


@app.command()
def download(
    slug: str = typer.Argument(  # noqa: B008
        ..., help="Dataset slug (e.g. SINAN)"
    ),
    group: str = typer.Option(None, help="Group/disease code"),  # noqa: B008
    state: str = typer.Option(None, help="Two-letter state"),  # noqa: B008
    year: int = typer.Option(None, help="Year to download"),  # noqa: B008
    output: str = typer.Option(  # noqa: B008
        None, "-o", "--output", help="Output directory"
    ),
):
    """Download files for a dataset with optional filters."""
    import pathlib

    from pysus import CACHEPATH
    from pysus.api.ftp.databases import AVAILABLE_DATABASES

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

    ftp = _get_ftp()
    try:
        datasets = ftp.datasets()
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

        typer.echo(f"Downloading {len(remote_files)} file(s) to {out_dir}...")
        for f in remote_files:
            typer.echo(f"  {f.path}")
            ftp.download(f, out_dir)

        typer.echo(f"\nDone. Files saved to {out_dir}")
    finally:
        ftp.close()
