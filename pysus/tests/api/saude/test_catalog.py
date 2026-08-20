"""Tests for the catalog fetcher (Next.js data layer)."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock

import httpx
import pytest
from pysus.api.saude.catalog import (
    _build_search_params,
    fetch_catalog_all,
    fetch_catalog_page,
    fetch_dataset,
    fetch_json,
    list_groups,
    list_tags,
)
from pysus.api.saude.errors import DatasetNotFound, PortalChanged


class TestFetchCatalogPage:
    @pytest.mark.asyncio
    async def test_returns_catalog_page(
        self, tmp_path, mocked_saude, saude_homepage_html
    ):
        async with httpx.AsyncClient(transport=mocked_saude) as client:
            page = await fetch_catalog_page(
                client,
                build_id="test-build-id",
                page=1,
                cache_root=tmp_path,
                ttl=__import__("datetime").timedelta(hours=24),
            )
        assert page.number_of_packages == 138
        assert page.page == 1
        assert len(page.packages) == 20
        names = [p.name for p in page.packages]
        assert "arboviroses-dengue" in names

    @pytest.mark.asyncio
    async def test_raises_portal_changed_on_bad_shape(self, tmp_path):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b'{"pageProps": {}}')

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(PortalChanged):
                await fetch_catalog_page(
                    client,
                    build_id="x",
                    page=1,
                    cache_root=tmp_path,
                    ttl=__import__("datetime").timedelta(hours=24),
                )


class TestFetchDataset:
    @pytest.mark.asyncio
    async def test_returns_package(self, tmp_path, mocked_saude):
        async with httpx.AsyncClient(transport=mocked_saude) as client:
            package = await fetch_dataset(
                client,
                build_id="test-build-id",
                slug="arboviroses-dengue",
                cache_root=tmp_path,
                ttl=__import__("datetime").timedelta(hours=24),
            )
        assert package.name == "arboviroses-dengue"
        assert len(package.resources) == 83
        assert package.periodicity == "Semanal"

    @pytest.mark.asyncio
    async def test_raises_dataset_not_found(self, tmp_path):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200, content=b'{"pageProps": {"name": "other"}}'
            )

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(DatasetNotFound):
                await fetch_dataset(
                    client,
                    build_id="x",
                    slug="missing",
                    cache_root=tmp_path,
                    ttl=__import__("datetime").timedelta(hours=24),
                )

    @pytest.mark.asyncio
    async def test_raises_portal_changed_on_bad_payload(self, tmp_path):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200, content=b'{"pageProps": {"name": "x", "bogus": true}}'
            )

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(PortalChanged):
                await fetch_dataset(
                    client,
                    build_id="x",
                    slug="x",
                    cache_root=tmp_path,
                    ttl=__import__("datetime").timedelta(hours=24),
                )


class TestListGroups:
    @pytest.mark.asyncio
    async def test_returns_14_groups(self, tmp_path, mocked_saude):
        async with httpx.AsyncClient(transport=mocked_saude) as client:
            groups = await list_groups(
                client,
                build_id="test-build-id",
                cache_root=tmp_path,
                ttl=__import__("datetime").timedelta(hours=24),
            )
        assert len(groups) == 14
        names = {g.name for g in groups}
        assert "arboviroses" in names
        assert "saude-indigena" in names
        assert "vacinacao" in names


class TestListTags:
    @pytest.mark.asyncio
    async def test_returns_tag_list(self, tmp_path, mocked_saude):
        async with httpx.AsyncClient(transport=mocked_saude) as client:
            tags = await list_tags(
                client,
                build_id="test-build-id",
                cache_root=tmp_path,
                ttl=__import__("datetime").timedelta(hours=24),
            )
        assert len(tags) > 0
        assert all(t.name for t in tags)


class TestBuildSearchParams:
    def test_q_param(self):
        params = _build_search_params(q="dengue")
        assert params["q"] == "dengue"

    def test_tag_param(self):
        params = _build_search_params(tag="arboviroses")
        assert params["tags"] == "arboviroses"

    def test_fmt_param(self):
        params = _build_search_params(fmt="CSV")
        assert params["res_format"] == "CSV"

    def test_all_params(self):
        params = _build_search_params(
            q="dengue",
            group="arboviroses",
            tag="public",
            fmt="CSV",
            page=2,
        )
        assert params["q"] == "dengue"
        assert params["groups"] == "arboviroses"
        assert params["tags"] == "public"
        assert params["res_format"] == "CSV"
        assert params["page"] == 2

    def test_none_params_excluded(self):
        params = _build_search_params()
        assert params == {"page": 1}


class TestFetchJson:
    @pytest.mark.asyncio
    async def test_cache_path_none_uses_tmp(self, tmp_path):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={"ok": True},
                request=httpx.Request("GET", "https://test/"),
            )

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            data = await fetch_json(
                client,
                "https://test/api",
                cache_path=None,
                ttl=timedelta(hours=1),
                retries=1,
                use_cache=False,
            )
        assert data == {"ok": True}

    @pytest.mark.asyncio
    async def test_retry_on_transport_error(self):
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise httpx.ConnectError("refused")
            return httpx.Response(
                200,
                json={"ok": True},
                request=httpx.Request("GET", "https://test/"),
            )

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            data = await fetch_json(
                client,
                "https://test/api",
                cache_path=None,
                ttl=timedelta(hours=1),
                retries=3,
                use_cache=False,
            )
        assert data == {"ok": True}
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_all_retries_exhausted_raises(self):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused")

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(httpx.ConnectError):
                await fetch_json(
                    client,
                    "https://test/api",
                    cache_path=None,
                    ttl=timedelta(hours=1),
                    retries=2,
                    use_cache=False,
                )

    @pytest.mark.asyncio
    async def test_raises_portal_changed_on_bad_shape(self, tmp_path):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=b'{"pageProps": {}}',
                request=httpx.Request("GET", "https://test/"),
            )

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(PortalChanged):
                await fetch_catalog_page(
                    client,
                    build_id="x",
                    page=1,
                    cache_root=tmp_path,
                    ttl=timedelta(hours=1),
                )

    @pytest.mark.asyncio
    async def test_portal_changed_on_invalid_packages(self, tmp_path):
        """Lines 178-179: packages present but model_validate fails."""
        import json as _json

        payload = _json.dumps(
            {
                "pageProps": {
                    "packages": "not-a-list",
                    "numberOfPackages": 0,
                    "currentFilters": {},
                    "availableFilters": {},
                }
            }
        ).encode()

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=payload,
                request=httpx.Request("GET", "https://test/"),
            )

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(PortalChanged, match="Could not parse"):
                await fetch_catalog_page(
                    client,
                    build_id="x",
                    page=1,
                    cache_root=tmp_path,
                    ttl=timedelta(hours=1),
                )


class TestFetchCatalogAll:
    @pytest.mark.asyncio
    async def test_max_pages_limits_output(self, tmp_path, mocked_saude):
        async with httpx.AsyncClient(transport=mocked_saude) as client:
            entries = []
            async for entry in fetch_catalog_all(
                client,
                build_id="test-build-id",
                cache_root=tmp_path,
                ttl=timedelta(hours=24),
                max_pages=1,
            ):
                entries.append(entry)
        assert len(entries) == 20

    @pytest.mark.asyncio
    async def test_empty_packages_stops(self, tmp_path):
        from unittest.mock import patch

        from pysus.api.saude.resources import CatalogPage

        empty_page = CatalogPage.model_validate(
            {
                "page": 1,
                "rows": 20,
                "numberOfPackages": 0,
                "currentFilters": {},
                "availableFilters": {},
                "packages": [],
            }
        )

        async def fake_page(*args, **kwargs):
            return empty_page

        with patch(
            "pysus.api.saude.catalog.fetch_catalog_page",
            side_effect=fake_page,
        ):
            mock_client = AsyncMock()
            entries = []
            async for entry in fetch_catalog_all(
                mock_client,
                build_id="x",
                cache_root=tmp_path,
                ttl=timedelta(hours=1),
            ):
                entries.append(entry)
        assert entries == []
