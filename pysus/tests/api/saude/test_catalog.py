"""Tests for the catalog fetcher (Next.js data layer)."""

from __future__ import annotations

import httpx
import pytest
from pysus.api.saude.catalog import (
    fetch_catalog_page,
    fetch_dataset,
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
