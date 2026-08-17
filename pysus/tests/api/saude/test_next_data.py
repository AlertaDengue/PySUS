"""Tests for the Next.js buildId extractor."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import pytest
from pysus.api.saude.errors import BuildIdMissing, NoUsableBuildId
from pysus.api.saude.next_data import (
    _is_fresh,
    _parse_build_id,
    _read_cache,
    _write_cache,
    fetch_build_id,
)


class TestParseBuildId:
    def test_extracts_build_id_from_homepage(self, saude_homepage_html: str):
        build_id = _parse_build_id(saude_homepage_html)
        assert isinstance(build_id, str)
        assert len(build_id) >= 8

    def test_raises_when_script_tag_missing(self):
        html = "<html><body>no script tag here</body></html>"
        with pytest.raises(BuildIdMissing, match="__NEXT_DATA__"):
            _parse_build_id(html)

    def test_raises_when_payload_lacks_build_id(self):
        html = (
            '<script id="__NEXT_DATA__" type="application/json">'
            '{"props": {}}</script>'
        )
        with pytest.raises(BuildIdMissing, match="buildId"):
            _parse_build_id(html)

    def test_raises_when_payload_is_invalid_json(self):
        html = (
            '<script id="__NEXT_DATA__" type="application/json">'
            "not json</script>"
        )
        with pytest.raises(BuildIdMissing):
            _parse_build_id(html)


class TestCache:
    def test_roundtrip(self, tmp_path: Path):
        path = tmp_path / "build_id.json"
        _write_cache(path, "abc123", datetime.now())
        loaded = _read_cache(path)
        assert loaded is not None
        assert loaded["buildId"] == "abc123"
        assert "saved_at" in loaded

    def test_read_missing_returns_none(self, tmp_path: Path):
        assert _read_cache(tmp_path / "missing.json") is None

    def test_is_fresh_true_for_recent(self, tmp_path: Path):
        path = tmp_path / "build_id.json"
        _write_cache(path, "abc", datetime.now())
        assert _is_fresh(path, timedelta(hours=1), datetime.now())

    def test_is_fresh_false_for_old(self, tmp_path: Path):
        path = tmp_path / "build_id.json"
        _write_cache(path, "abc", datetime.now() - timedelta(hours=2))
        assert not _is_fresh(path, timedelta(hours=1), datetime.now())

    def test_is_fresh_false_for_missing(self, tmp_path: Path):
        assert not _is_fresh(
            tmp_path / "missing.json", timedelta(hours=1), datetime.now()
        )

    def test_is_fresh_false_for_garbage(self, tmp_path: Path):
        path = tmp_path / "build_id.json"
        path.write_text("{not json")
        assert not _is_fresh(path, timedelta(hours=1), datetime.now())


class TestFetchBuildId:
    @pytest.mark.asyncio
    async def test_uses_cached_when_fresh(
        self, tmp_path: Path, saude_homepage_html: str
    ):
        transport = httpx.MockTransport(
            lambda req: httpx.Response(
                200, content=saude_homepage_html.encode()
            )
        )
        async with httpx.AsyncClient(transport=transport) as client:
            cache_path = tmp_path / "build_id.json"
            _write_cache(cache_path, "cached-id", datetime.now())
            build_id = await fetch_build_id(
                client,
                cache_path=cache_path,
                homepage_url="https://dadosabertos.saude.gov.br/",
            )
        assert build_id == "cached-id"

    @pytest.mark.asyncio
    async def test_refetches_when_stale(
        self, tmp_path: Path, saude_homepage_html: str
    ):
        transport = httpx.MockTransport(
            lambda req: httpx.Response(
                200, content=saude_homepage_html.encode()
            )
        )
        async with httpx.AsyncClient(transport=transport) as client:
            cache_path = tmp_path / "build_id.json"
            _write_cache(
                cache_path, "stale-id", datetime.now() - timedelta(hours=48)
            )
            build_id = await fetch_build_id(
                client,
                cache_path=cache_path,
                homepage_url="https://dadosabertos.saude.gov.br/",
            )
        assert build_id != "stale-id"
        # The cache was overwritten with the fresh value
        loaded = json.loads(cache_path.read_text())
        assert loaded["buildId"] == build_id

    @pytest.mark.asyncio
    async def test_falls_back_to_stale_cache_on_homepage_failure(
        self, tmp_path: Path
    ):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, content=b"service unavailable")

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            cache_path = tmp_path / "build_id.json"
            _write_cache(cache_path, "stale-id", datetime.now())
            build_id = await fetch_build_id(
                client,
                cache_path=cache_path,
                homepage_url="https://dadosabertos.saude.gov.br/",
            )
        assert build_id == "stale-id"

    @pytest.mark.asyncio
    async def test_raises_when_no_cache_and_homepage_fails(
        self, tmp_path: Path
    ):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, content=b"service unavailable")

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(NoUsableBuildId):
                await fetch_build_id(
                    client,
                    cache_path=tmp_path / "build_id.json",
                    homepage_url="https://dadosabertos.saude.gov.br/",
                )
