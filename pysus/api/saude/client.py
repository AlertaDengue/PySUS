"""Async facade for the Saude (dadosabertos.saude.gov.br) client."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from datetime import timedelta
from pathlib import Path

import httpx
from pysus import CACHEPATH

from .catalog import (
    _DEFAULT_TTL,
    fetch_build_id,
    fetch_catalog_all,
    fetch_catalog_page,
    fetch_dataset,
    list_groups,
    list_tags,
)
from .download import download_dataset as _download_dataset
from .download import download_resource as _download_resource
from .resources import CatalogEntry, CKANPackage, GroupRef, Resource, TagRef


class SaudeClient:
    """Async client for the OpenDataSUS portal.

    The portal is a Next.js frontend over a CKAN backend. ``SaudeClient``
    owns an ``httpx.AsyncClient`` and the on-disk caches for the Next.js
    ``buildId`` and the catalog pages. No authentication is required.

    Example
    -------
    >>> import asyncio
    >>> from pysus.api.saude import SaudeClient
    >>> async def main():
    ...     async with SaudeClient() as c:
    ...         datasets = await c.list_datasets(group="arboviroses")
    ...         print([d.name for d in datasets])
    >>> asyncio.run(main())
    """

    BASE_URL = "https://dadosabertos.saude.gov.br"

    def __init__(
        self,
        *,
        cache_dir: Path | None = None,
        cache_ttl: timedelta = _DEFAULT_TTL,
        timeout: float = 30.0,
        user_agent: str | None = None,
    ) -> None:
        self.cache_dir = (
            Path(cache_dir) if cache_dir else Path(CACHEPATH) / "saude"
        )
        self.cache_ttl = cache_ttl
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        headers = {"User-Agent": user_agent or "pysus-saude/0.1 (research)"}
        self._client = httpx.AsyncClient(
            headers=headers, timeout=timeout, follow_redirects=True
        )
        self._build_id: str | None = None

    async def __aenter__(self) -> SaudeClient:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    async def _ensure_build_id(self, use_cache: bool = True) -> str:
        if self._build_id and not use_cache:
            pass
        elif self._build_id and use_cache:
            return self._build_id
        build_id = await fetch_build_id(
            self._client,
            cache_path=self.cache_dir / "build_id.json",
            homepage_url=self.BASE_URL + "/",
            ttl=self.cache_ttl,
        )
        self._build_id = build_id
        return build_id

    async def list_datasets(
        self,
        *,
        q: str | None = None,
        group: str | None = None,
        tag: str | None = None,
        fmt: str | None = None,
        page: int = 1,
        use_cache: bool = True,
    ) -> list[CatalogEntry]:
        """Return one page (20 entries) of the catalog listing."""
        build_id = await self._ensure_build_id(use_cache=use_cache)
        catalog = await fetch_catalog_page(
            self._client,
            build_id=build_id,
            q=q,
            group=group,
            tag=tag,
            fmt=fmt,
            page=page,
            cache_root=self.cache_dir,
            ttl=self.cache_ttl,
            use_cache=use_cache,
        )
        return catalog.packages

    async def iter_datasets(
        self,
        *,
        q: str | None = None,
        group: str | None = None,
        tag: str | None = None,
        fmt: str | None = None,
        max_pages: int | None = None,
        use_cache: bool = True,
    ) -> AsyncIterator[CatalogEntry]:
        """Yield every catalog entry across all pages."""
        build_id = await self._ensure_build_id(use_cache=use_cache)
        async for entry in fetch_catalog_all(
            self._client,
            build_id=build_id,
            q=q,
            group=group,
            tag=tag,
            fmt=fmt,
            max_pages=max_pages,
            cache_root=self.cache_dir,
            ttl=self.cache_ttl,
            use_cache=use_cache,
        ):
            yield entry

    async def list_groups(self, *, use_cache: bool = True) -> list[GroupRef]:
        """Return the 14 catalog groups (themes)."""
        build_id = await self._ensure_build_id(use_cache=use_cache)
        return await list_groups(
            self._client,
            build_id=build_id,
            cache_root=self.cache_dir,
            ttl=self.cache_ttl,
            use_cache=use_cache,
        )

    async def list_tags(self, *, use_cache: bool = True) -> list[TagRef]:
        """Return the catalog tags."""
        build_id = await self._ensure_build_id(use_cache=use_cache)
        return await list_tags(
            self._client,
            build_id=build_id,
            cache_root=self.cache_dir,
            ttl=self.cache_ttl,
            use_cache=use_cache,
        )

    async def fetch_dataset(
        self, slug: str, *, use_cache: bool = True
    ) -> CKANPackage:
        """Fetch the full CKAN package for a single dataset."""
        build_id = await self._ensure_build_id(use_cache=use_cache)
        return await fetch_dataset(
            self._client,
            build_id=build_id,
            slug=slug,
            cache_root=self.cache_dir,
            ttl=self.cache_ttl,
            use_cache=use_cache,
        )

    async def fetch_resources(
        self, slug: str, *, use_cache: bool = True
    ) -> list[Resource]:
        """Fetch the resources of a dataset."""
        package = await self.fetch_dataset(slug, use_cache=use_cache)
        return package.resources

    async def download_resource(
        self,
        slug: str,
        *,
        resource_id: str | None = None,
        name: str | None = None,
        fmt: str | None = None,
        dest_dir: Path | None = None,
        progress: Callable[[int, int], None] | None = None,
        overwrite: bool = False,
        use_cache: bool = True,
    ) -> Path:
        """Download one resource of a dataset."""
        package = await self.fetch_dataset(slug, use_cache=use_cache)
        return await _download_resource(
            self._client,
            package,
            resource_id=resource_id,
            name=name,
            fmt=fmt,
            dest_dir=dest_dir,
            progress=progress,
            overwrite=overwrite,
        )

    async def download_dataset(
        self,
        slug: str,
        *,
        dest_dir: Path | None = None,
        fmt: str | None = None,
        progress: Callable[[int, int], None] | None = None,
        overwrite: bool = False,
        use_cache: bool = True,
    ) -> list[Path]:
        """Download every downloadable resource of a dataset."""
        package = await self.fetch_dataset(slug, use_cache=use_cache)
        return await _download_dataset(
            self._client,
            package,
            dest_dir=dest_dir,
            fmt=fmt,
            progress=progress,
            overwrite=overwrite,
        )


__all__ = ["SaudeClient"]
