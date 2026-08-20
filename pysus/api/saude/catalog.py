"""Async Next.js data fetcher for dadosabertos.saude.gov.br.

Talks to the portal's data layer of the form::

    GET /_next/data/<buildId>/dataset.json?q=&groups=&tags=&res_format=&page=
    GET /_next/data/<buildId>/dataset/<slug>.json?slug=<slug>

Each call goes through :func:`fetch_json` which handles disk caching
(TTL) and exponential-backoff retries on transient transport errors.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from .errors import DatasetNotFound, PortalChanged
from .next_data import fetch_build_id
from .resources import CatalogEntry, CatalogPage, CKANPackage, GroupRef, TagRef

logger = logging.getLogger(__name__)

PAGE_SIZE = 20
_DEFAULT_TTL = timedelta(hours=24)
_RETRYABLE = (httpx.TransportError, httpx.HTTPStatusError, httpx.RequestError)


def _cache_key(url: str, params: dict[str, Any] | None) -> str:
    """Stable cache key from the URL + sorted query params."""
    parts = [url.replace("https://", "").replace("/", "_").replace(":", "_")]
    if params:
        parts.append("_".join(f"{k}-{v}" for k, v in sorted(params.items())))
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return digest


def _is_fresh(path: Path, ttl: timedelta, now: datetime) -> bool:
    if not path.exists():
        return False
    age = now - datetime.fromtimestamp(path.stat().st_mtime)
    return age < ttl


def _read_cache(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def _write_cache(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh)


async def fetch_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    cache_path: Path | None = None,
    ttl: timedelta = _DEFAULT_TTL,
    use_cache: bool = True,
    retries: int = 3,
) -> dict[str, Any]:
    """GET a JSON payload with TTL cache and exponential-backoff retries.

    Parameters
    ----------
    client : httpx.AsyncClient
    url : str
    params : dict, optional
    cache_path : pathlib.Path, optional
        Override the auto-derived cache path.
    ttl : datetime.timedelta, optional
    use_cache : bool, optional
    retries : int, optional

    Returns
    -------
    dict
        Parsed JSON response.
    """
    path = cache_path
    if path is None:
        path = Path(f"/tmp/saude-{_cache_key(url, params)}.json")
    if use_cache and _is_fresh(path, ttl, datetime.now()):
        logger.debug("saude: cache hit for %s", url)
        return _read_cache(path)

    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        except _RETRYABLE as exc:
            last_error = exc
            wait = 2**attempt + attempt
            logger.warning(
                "saude: GET %s attempt %d/%d failed: %s. Retrying in %ds.",
                url,
                attempt + 1,
                retries,
                exc,
                wait,
            )
            await asyncio.sleep(wait)
            continue
        if use_cache:
            _write_cache(path, data)
        return data

    raise last_error if last_error else RuntimeError("unreachable")


def _build_search_params(
    q: str | None = None,
    group: str | None = None,
    tag: str | None = None,
    fmt: str | None = None,
    page: int = 1,
) -> dict[str, Any]:
    """Mirror of the epidatasets ``_search_params`` helper."""
    params: dict[str, Any] = {"page": page}
    if q:
        params["q"] = q
    if group:
        params["groups"] = group
    if tag:
        params["tags"] = tag
    if fmt:
        params["res_format"] = fmt
    return params


async def fetch_catalog_page(
    client: httpx.AsyncClient,
    *,
    build_id: str,
    q: str | None = None,
    group: str | None = None,
    tag: str | None = None,
    fmt: str | None = None,
    page: int = 1,
    cache_root: Path,
    ttl: timedelta,
    use_cache: bool = True,
) -> CatalogPage:
    """Fetch one page (20 datasets) of the catalog listing."""
    base = "https://dadosabertos.saude.gov.br"
    url = f"{base}/_next/data/{build_id}/dataset.json"
    params = _build_search_params(q, group, tag, fmt, page)
    cache_path = cache_root / "catalog" / f"{_cache_key(url, params)}.json"
    data = await fetch_json(
        client,
        url,
        params=params,
        cache_path=cache_path,
        ttl=ttl,
        use_cache=use_cache,
    )
    page_props = data.get("pageProps", {})
    if "packages" not in page_props:
        raise PortalChanged(
            "Catalog response does not contain 'packages'; the portal "
            "frontend data format may have changed."
        )
    try:
        return CatalogPage.model_validate(page_props)
    except Exception as exc:  # noqa: B902 — wrap any validation error
        raise PortalChanged(
            f"Could not parse catalog page payload: {exc}"
        ) from exc


async def fetch_catalog_all(
    client: httpx.AsyncClient,
    *,
    build_id: str,
    q: str | None = None,
    group: str | None = None,
    tag: str | None = None,
    fmt: str | None = None,
    max_pages: int | None = None,
    cache_root: Path,
    ttl: timedelta,
    use_cache: bool = True,
) -> AsyncIterator[CatalogEntry]:
    """Yield every catalog entry across all pages.

    Stops when a page returns no packages, or when the number of
    packages so far equals ``numberOfPackages``.
    """
    page = 1
    yielded = 0
    total: int | None = None
    while True:
        if max_pages is not None and page > max_pages:
            logger.warning(
                "saude: reached max_pages=%d; results may be incomplete.",
                max_pages,
            )
            return
        catalog = await fetch_catalog_page(
            client,
            build_id=build_id,
            q=q,
            group=group,
            tag=tag,
            fmt=fmt,
            page=page,
            cache_root=cache_root,
            ttl=ttl,
            use_cache=use_cache,
        )
        if not catalog.packages:
            return
        if total is None:
            total = catalog.number_of_packages
        for entry in catalog.packages:
            yield entry
            yielded += 1
        if total and yielded >= total:
            return
        page += 1


async def fetch_dataset(
    client: httpx.AsyncClient,
    *,
    build_id: str,
    slug: str,
    cache_root: Path,
    ttl: timedelta,
    use_cache: bool = True,
) -> CKANPackage:
    """Fetch the full CKAN package for a single dataset."""
    base = "https://dadosabertos.saude.gov.br"
    url = f"{base}/_next/data/{build_id}/dataset/{slug}.json"
    cache_path = cache_root / "dataset" / f"{slug}.json"
    data = await fetch_json(
        client,
        url,
        params={"slug": slug},
        cache_path=cache_path,
        ttl=ttl,
        use_cache=use_cache,
    )
    page_props = data.get("pageProps") or {}
    if not page_props or page_props.get("name") != slug:
        raise DatasetNotFound(f"Dataset '{slug}' not found on OpenDataSUS.")
    try:
        return CKANPackage.model_validate(page_props)
    except Exception as exc:  # noqa: B902 — wrap any validation error
        raise PortalChanged(
            f"Could not parse dataset payload for '{slug}': {exc}"
        ) from exc


async def list_groups(
    client: httpx.AsyncClient,
    *,
    build_id: str,
    cache_root: Path,
    ttl: timedelta,
    use_cache: bool = True,
) -> list[GroupRef]:
    """Return all 14 catalog groups (themes)."""
    page = await fetch_catalog_page(
        client,
        build_id=build_id,
        page=1,
        cache_root=cache_root,
        ttl=ttl,
        use_cache=use_cache,
    )
    return [
        GroupRef.model_validate(g)
        for g in page.available_filters.get("groups", [])
    ]


async def list_tags(
    client: httpx.AsyncClient,
    *,
    build_id: str,
    cache_root: Path,
    ttl: timedelta,
    use_cache: bool = True,
) -> list[TagRef]:
    """Return all catalog tags."""
    page = await fetch_catalog_page(
        client,
        build_id=build_id,
        page=1,
        cache_root=cache_root,
        ttl=ttl,
        use_cache=use_cache,
    )
    return [
        TagRef.model_validate(t) for t in page.available_filters.get("tags", [])
    ]


__all__ = [
    "PAGE_SIZE",
    "fetch_build_id",
    "fetch_catalog_page",
    "fetch_catalog_all",
    "fetch_dataset",
    "list_groups",
    "list_tags",
]
