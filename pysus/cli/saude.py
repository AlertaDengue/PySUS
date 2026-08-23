"""Typer sub-app for the OpenDataSUS catalog (dadosabertos.saude.gov.br).

Commands:

- ``pysus saude list-datasets``  — one page of the catalog
- ``pysus saude list-groups``    — the 14 catalog themes
- ``pysus saude show``           — full CKAN metadata of a dataset
- ``pysus saude download``       — download a dataset's resources
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import typer

app = typer.Typer(help="OpenDataSUS catalog (dadosabertos.saude.gov.br)")


def _run(coro):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    return loop.run_until_complete(coro)


@app.command("list-datasets")
def list_datasets(
    group: str = typer.Option(  # noqa: B008
        None, "--group", help="Filter by group slug"
    ),
    tag: str = typer.Option(None, "--tag", help="Filter by tag"),  # noqa: B008
    fmt: str = typer.Option(  # noqa: B008
        None, "--fmt", help="Filter by resource format"
    ),
    q: str = typer.Option(None, "--q", help="Free-text search"),  # noqa: B008
    page: int = typer.Option(  # noqa: B008
        1, "--page", help="Page number (20 per page)"
    ),
):
    """List one page of the OpenDataSUS catalog."""

    from pysus.api.saude import SaudeClient

    async def _main():
        async with SaudeClient() as c:
            entries = await c.list_datasets(
                group=group, tag=tag, fmt=fmt, q=q, page=page
            )
            for e in entries:
                formats = ", ".join(e.formats)
                print(f"{e.name:50s}  {e.title[:60]:60s}  [{formats}]")

    _run(_main())


@app.command("list-groups")
def list_groups():
    """List the catalog groups (themes)."""

    from pysus.api.saude import SaudeClient

    async def _main():
        async with SaudeClient() as c:
            for g in await c.list_groups():
                print(f"{g.name:35s}  {g.display_name or ''}")

    _run(_main())


@app.command("show")
def show(slug: str = typer.Argument(..., help="Dataset slug")):  # noqa: B008
    """Show the full CKAN metadata of a dataset."""

    from pysus.api.saude import SaudeClient

    async def _main():
        async with SaudeClient() as c:
            pkg = await c.fetch_dataset(slug)
            print(f"title: {pkg.title}")
            print(f"slug: {pkg.name}")
            print(f"id (UUID): {pkg.id}")
            org_name = (
                pkg.organization.display_name if pkg.organization else "?"
            )
            print(f"organization: {org_name}")
            print(f"license: {pkg.license_title or '?'}")
            print(f"created: {pkg.metadata_created}")
            print(f"modified: {pkg.metadata_modified}")
            print(f"periodicity: {pkg.periodicity or '?'}")
            print(f"contact: {pkg.contact or '?'}")
            print(f"resources: {pkg.num_resources}")
            groups = ", ".join(g.display_name or g.name for g in pkg.groups)
            print(f"groups: {groups}")
            print()
            print("notes:")
            print(pkg.notes or "(empty)")

    _run(_main())


@app.command("download")
def download(
    slug: str = typer.Argument(..., help="Dataset slug"),  # noqa: B008
    fmt: str = typer.Option(  # noqa: B008
        None, "--fmt", help="Only download this format"
    ),
    dest: Path = typer.Option(  # noqa: B008
        None, "--dest", help="Destination directory"
    ),
):
    """Download all downloadable resources of a dataset."""

    from pysus.api.saude import SaudeClient

    async def _main():
        async with SaudeClient() as c:
            paths = await c.download_dataset(slug, dest_dir=dest, fmt=fmt)
            for p in paths:
                print(p)

    _run(_main())
