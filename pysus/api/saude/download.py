"""Async download helpers for Saude (CKAN) resources.

Mirrors ``epidatasets.sources.opendatasus.OpenDataSUSAccessor
.download_resource`` / ``.download_dataset`` but uses ``httpx.AsyncClient
.stream`` and a progress callback shaped like the rest of PySUS.

Resources whose ``format`` is ``API`` are documentation links and are
skipped automatically.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlparse

from .errors import ResourceNotFound
from .resources import CKANPackage, Resource

_DOWNLOAD_CHUNK = 1 << 16
_NON_SAFE = re.compile(r"[^\w.\- ]")


def filename_for(resource: Resource, slug: str, index: int) -> str:
    """Derive a safe filename for a resource download.

    Ported verbatim from the epidatasets reference implementation.
    """
    base = (
        resource.name
        or Path(urlparse(resource.url).path).name
        or f"resource_{index}"
    )
    base = _NON_SAFE.sub("_", base).strip() or f"resource_{index}"
    fmt = (resource.format or "").strip().lower()
    if fmt and fmt != "api" and not base.lower().endswith(f".{fmt}"):
        base = f"{base}.{fmt}"
    return base


def _select_resource(
    package: CKANPackage,
    *,
    resource_id: str | None,
    name: str | None,
    fmt: str | None,
) -> Resource:
    """Pick the unique resource matching the selector, else raise."""
    if (resource_id is None) == (name is None):
        raise ValueError("Provide exactly one of 'resource_id' or 'name'.")
    matches = []
    for resource in package.resources:
        if resource_id is not None and resource.id != resource_id:
            continue
        if name is not None and resource.name != name:
            continue
        if fmt is not None and resource.format.upper() != fmt.upper():
            continue
        if resource.format.upper() == "API":
            continue
        matches.append(resource)
    if not matches:
        raise ResourceNotFound(
            f"No downloadable resource matches resource_id={resource_id!r}, "
            f"name={name!r}, fmt={fmt!r} for dataset '{package.name}'.",
            candidates=len(package.resources),
        )
    return matches[0]


async def download_resource(
    client,  # httpx.AsyncClient
    package: CKANPackage,
    *,
    resource_id: str | None = None,
    name: str | None = None,
    fmt: str | None = None,
    dest_dir: Path | None = None,
    progress: Callable[[int, int], None] | None = None,
    overwrite: bool = False,
) -> Path:
    """Download a single resource of a dataset to disk.

    Parameters
    ----------
    client : httpx.AsyncClient
    package : CKANPackage
    resource_id, name, fmt : optional selectors (exactly one of id/name)
    dest_dir : pathlib.Path, optional
        Defaults to ``<cache_dir>/downloads/<slug>/``.
    progress : callable, optional
        ``(downloaded_bytes, total_bytes)`` callback.
    overwrite : bool, optional
        When ``False`` (default), an existing file is reused.

    Returns
    -------
    pathlib.Path
        Path to the downloaded file.
    """
    resource = _select_resource(
        package, resource_id=resource_id, name=name, fmt=fmt
    )
    target_dir = (
        Path(dest_dir) if dest_dir else Path(f"./{package.name}-downloads")
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    index = resource.position or 0
    dest_path = target_dir / filename_for(resource, package.name, index)

    if dest_path.exists() and not overwrite:
        return dest_path

    async with client.stream("GET", resource.url) as response:
        response.raise_for_status()
        total = int(response.headers.get("Content-Length", 0))
        downloaded = 0
        with dest_path.open("wb") as fh:
            async for chunk in response.aiter_bytes(chunk_size=_DOWNLOAD_CHUNK):
                fh.write(chunk)
                downloaded += len(chunk)
                if progress:
                    progress(downloaded, total)
    return dest_path


async def download_dataset(
    client,  # httpx.AsyncClient
    package: CKANPackage,
    *,
    dest_dir: Path | None = None,
    fmt: str | None = None,
    progress: Callable[[int, int], None] | None = None,
    overwrite: bool = False,
) -> list[Path]:
    """Download every downloadable resource of a dataset.

    Returns the list of paths written. API-format resources are
    skipped.
    """
    dest = Path(dest_dir) if dest_dir else Path(f"./{package.name}-downloads")
    dest.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    fmt_upper = fmt.upper() if fmt else None
    for resource in package.resources:
        if resource.format.upper() == "API":
            continue
        if fmt_upper and resource.format.upper() != fmt_upper:
            continue
        path = await download_resource(
            client,
            package,
            resource_id=resource.id,
            dest_dir=dest,
            progress=progress,
            overwrite=overwrite,
        )
        paths.append(path)
    return paths


__all__ = [
    "filename_for",
    "download_resource",
    "download_dataset",
]
