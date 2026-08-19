"""Helpers for the DEMAS REST API (apidadosabertos.saude.gov.br).

The DEMAS (Dados Estatísticos e Metadados em Saúde) API serves
paginated JSON endpoints for epidemiological and institutional data.
This module provides:

- :class:`EndpointSpec` — a frozen descriptor for one REST endpoint;
- :func:`fetch_swagger` — download and cache the OpenAPI spec;
- :func:`iter_rows` — async paginator that yields row dicts;
- :func:`endpoints_from_swagger` — extract endpoint specs for a dataset.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)

DEMAS_BASE = "https://apidadosabertos.saude.gov.br"
_SWAGGER_PATH = "/static/swagger.json"
_DEFAULT_PAGE_SIZE = 1000
_DEFAULT_SWAGGER_TTL = timedelta(hours=1)


@dataclass(frozen=True)
class EndpointSpec:
    """Descriptor for a single DEMAS REST endpoint.

    Attributes
    ----------
    path : str
        Absolute endpoint path, e.g. ``/arboviroses/dengue``.
    summary : str
        Human-readable description (from the swagger summary).
    params : tuple[str, ...]
        Query parameter names accepted by this endpoint.
    tag : str
        The swagger tag (DEMAs theme) this endpoint belongs to.
    limit : int
        Maximum rows per request (server cap is 1000).
    """

    path: str
    summary: str = ""
    params: tuple[str, ...] = ()
    tag: str = ""
    limit: int = _DEFAULT_PAGE_SIZE


# -- Swagger cache --------------------------------------------------------


def _swagger_cache_path(cache_root: Path) -> Path:
    return cache_root / "demas_swagger.json"


def _cache_age(path: Path, ttl: timedelta) -> bool:
    """Return ``True`` if the cached file is still fresh."""
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return False
    return (time.time() - mtime) < ttl.total_seconds()


async def fetch_swagger(
    client: httpx.AsyncClient,
    cache_root: Path,
    ttl: timedelta = _DEFAULT_SWAGGER_TTL,
    *,
    use_cache: bool = True,
) -> dict[str, Any]:
    """Fetch the DEMAS OpenAPI spec, returning a cached copy when fresh.

    Parameters
    ----------
    client : httpx.AsyncClient
        The HTTP client to use.
    cache_root : Path
        Directory where the swagger JSON is cached on disk.
    ttl : timedelta
        Maximum age of the cached file before re-fetching.
    use_cache : bool
        If ``False``, force a fresh download.

    Returns
    -------
    dict
        The parsed swagger JSON (OpenAPI 2.0).
    """
    cache_root.mkdir(parents=True, exist_ok=True)
    cached = _swagger_cache_path(cache_root)
    if use_cache and _cache_age(cached, ttl):
        try:
            return json.loads(cached.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            pass

    url = DEMAS_BASE + _SWAGGER_PATH
    logger.debug("Fetching swagger from %s", url)
    resp = await client.get(url)
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()
    cached.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return data


# -- Row paginator --------------------------------------------------------


def _extract_rows(data: Any) -> list[dict]:
    """Extract the row list from a DEMAS API response envelope.

    DEMAS responses have the shape ``{"<key>": [rows...]}`` where the
    key varies per endpoint.  Some endpoints return a bare list.
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for value in data.values():
            if isinstance(value, list):
                return value
    return []


async def iter_rows(
    client: httpx.AsyncClient,
    endpoint_path: str,
    *,
    params: dict[str, Any] | None = None,
    page_size: int = _DEFAULT_PAGE_SIZE,
    limit: int | None = None,
    _offset: int = 0,
) -> AsyncIterator[dict]:
    """Yield row dicts from a paginated DEMAS REST endpoint.

    The DEMAS API uses **row-offset** pagination: ``offset`` is the
    index of the first row to return (not a page number).  Each page
    returns at most ``page_size`` rows.  Iteration stops when the
    server returns fewer than ``page_size`` rows or an empty list.

    Parameters
    ----------
    client : httpx.AsyncClient
        The HTTP client to use.
    endpoint_path : str
        Absolute path, e.g. ``/arboviroses/dengue``.
    params : dict, optional
        Extra query parameters (e.g. ``{"nu_ano": "2024"}``).
    page_size : int
        Rows per request (capped at 1000 by the server).
    limit : int, optional
        Maximum total rows to yield across all pages.
    _offset : int
        Starting row offset (for resumption; normally 0).
    """
    offset = _offset
    total = 0
    while True:
        req_params: dict[str, Any] = {
            "limit": min(page_size, _DEFAULT_PAGE_SIZE),
            "offset": offset,
        }
        if params:
            req_params.update(params)
        resp = await client.get(DEMAS_BASE + endpoint_path, params=req_params)
        resp.raise_for_status()
        rows = _extract_rows(resp.json())
        if not rows:
            break
        for row in rows:
            yield row
            total += 1
            if limit is not None and total >= limit:
                return
        if len(rows) < page_size:
            break
        offset += len(rows)


# -- Swagger → EndpointSpec extraction -----------------------------------


def endpoints_from_swagger(
    swagger: dict[str, Any],
    tag: str | None = None,
) -> list[EndpointSpec]:
    """Extract :class:`EndpointSpec` objects from a parsed swagger dict.

    Parameters
    ----------
    swagger : dict
        The full OpenAPI 2.0 spec.
    tag : str, optional
        Filter by this swagger tag.  If ``None``, returns all
        endpoints.

    Returns
    -------
    list[EndpointSpec]
    """
    specs: list[EndpointSpec] = []
    paths = swagger.get("paths", {})
    for path, methods in sorted(paths.items()):
        get = methods.get("get")
        if get is None:
            continue
        ep_tags = get.get("tags", [])
        if tag and tag not in ep_tags:
            continue
        ep_tag = ep_tags[0] if ep_tags else ""
        summary = get.get("summary", "")
        params = tuple(
            p["name"]
            for p in get.get("parameters", [])
            if p.get("in") == "query"
        )
        specs.append(
            EndpointSpec(
                path=path,
                summary=summary,
                params=params,
                tag=ep_tag,
            )
        )
    return specs


__all__ = [
    "DEMAS_BASE",
    "EndpointSpec",
    "endpoints_from_swagger",
    "fetch_swagger",
    "iter_rows",
]
