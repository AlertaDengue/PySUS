"""Next.js data-layer helpers for dadosabertos.saude.gov.br.

The portal's CKAN backend is served through a Next.js frontend. The
CKAN Action API (``/api/3/action/*``) is not exposed, but Next.js
serves the same catalog data via its hydration endpoints of the form::

    /_next/data/<buildId>/dataset.json
    /_next/data/<buildId>/dataset/<slug>.json

``<buildId>`` rotates on every frontend deploy, so it must be
discovered from the homepage's ``__NEXT_DATA__`` script tag and
cached.

This module owns the buildId extraction and the on-disk cache. It
exposes one async helper (:func:`fetch_build_id`) and a small
sentinel value used by tests to bypass the cache.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path

import httpx

from .errors import BuildIdMissing, NoUsableBuildId

_NEXT_DATA_RE = re.compile(
    r'__NEXT_DATA__"\s+type="application/json">(.*?)</script>',
    re.DOTALL,
)

_DEFAULT_TTL = timedelta(hours=24)


def _parse_build_id(html: str) -> str:
    """Extract ``buildId`` from the ``__NEXT_DATA__`` script block.

    Parameters
    ----------
    html : str
        The full HTML of the homepage.

    Returns
    -------
    str
        The buildId value.

    Raises
    ------
    BuildIdMissing
        If the script tag or the ``buildId`` field cannot be located.
    """
    match = _NEXT_DATA_RE.search(html)
    if not match:
        raise BuildIdMissing(
            "Could not find __NEXT_DATA__ on the OpenDataSUS homepage; "
            "the portal frontend may have changed."
        )
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise BuildIdMissing(
            "Could not parse __NEXT_DATA__ JSON on the OpenDataSUS homepage."
        ) from exc
    build_id = payload.get("buildId")
    if not build_id:
        raise BuildIdMissing(
            "No 'buildId' field in the OpenDataSUS homepage __NEXT_DATA__."
        )
    return str(build_id)


def _read_cache(cache_path: Path) -> dict | None:
    """Return the cached buildId payload, or ``None`` if absent."""
    if not cache_path.exists():
        return None
    try:
        with cache_path.open(encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None


def _is_fresh(path: Path, ttl: timedelta, now: datetime) -> bool:
    """True when the cache payload at ``path`` is younger than ``ttl``.

    Freshness is decided by the ``saved_at`` timestamp recorded inside
    the payload — not the file mtime, which is not under our control
    (copies, git checkouts, etc. would skew it).
    """
    payload = _read_cache(path)
    if not payload:
        return False
    saved_at = payload.get("saved_at")
    if not saved_at:
        return False
    try:
        ts = datetime.fromisoformat(saved_at)
    except ValueError:
        return False
    return now - ts < ttl


def _write_cache(cache_path: Path, build_id: str, now: datetime) -> None:
    """Persist ``build_id`` to ``cache_path`` with a timestamp."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as fh:
        json.dump(
            {"buildId": build_id, "saved_at": now.isoformat()},
            fh,
        )


async def fetch_build_id(
    client: httpx.AsyncClient,
    *,
    cache_path: Path,
    homepage_url: str,
    ttl: timedelta = _DEFAULT_TTL,
) -> str:
    """Discover and cache the portal's current Next.js ``buildId``.

    The function consults the on-disk cache first, falls back to
    fetching the homepage if the cache is missing or stale, and writes
    the result back to the cache.

    Parameters
    ----------
    client : httpx.AsyncClient
        The HTTP client used for the homepage request.
    cache_path : pathlib.Path
        Where the buildId is cached on disk.
    homepage_url : str
        URL of the portal homepage.
    ttl : datetime.timedelta, optional
        How long a cached buildId is considered fresh.

    Returns
    -------
    str
        The current buildId.

    Raises
    ------
    NoUsableBuildId
        If neither the cache nor a fresh homepage fetch yields a value.
    """
    now = datetime.now()
    if _is_fresh(cache_path, ttl, now):
        cached = _read_cache(cache_path)
        cached_id = cached.get("buildId") if cached else None
        if cached_id:
            return str(cached_id)

    try:
        response = await client.get(homepage_url)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        stale = _read_cache(cache_path)
        stale_id = stale.get("buildId") if stale else None
        if stale_id:
            return str(stale_id)
        raise NoUsableBuildId(
            "Could not fetch the OpenDataSUS homepage and no cached "
            "buildId is available."
        ) from exc

    build_id = _parse_build_id(response.text)
    _write_cache(cache_path, build_id, now)
    return build_id
