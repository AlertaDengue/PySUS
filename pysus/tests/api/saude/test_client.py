"""Tests for the SaudeClient facade.

Wires the captured fixtures through ``httpx.MockTransport`` (via the
``saude_client`` fixture) and drives the public API end-to-end.
"""

from __future__ import annotations

import pytest
from pysus.api.saude import SaudeClient


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_context_manager_yields_self(self, tmp_path):
        async with SaudeClient(cache_dir=tmp_path, timeout=10.0) as c:
            assert isinstance(c, SaudeClient)


class TestListDatasets:
    @pytest.mark.asyncio
    async def test_returns_catalog_entries(self, saude_client: SaudeClient):
        page = await saude_client.list_datasets(group="arboviroses", page=1)
        assert len(page) == 20
        assert any(p.name == "arboviroses-dengue" for p in page)


class TestFetchDataset:
    @pytest.mark.asyncio
    async def test_returns_full_package(self, saude_client: SaudeClient):
        package = await saude_client.fetch_dataset("arboviroses-dengue")
        assert package.name == "arboviroses-dengue"
        assert len(package.resources) == 83
        assert package.periodicity == "Semanal"

    @pytest.mark.asyncio
    async def test_resources_are_parsed(self, saude_client: SaudeClient):
        resources = await saude_client.fetch_resources("arboviroses-dengue")
        formats = {r.format for r in resources}
        assert {"CSV", "JSON", "XML", "PDF", "API"} <= formats


class TestListGroups:
    @pytest.mark.asyncio
    async def test_returns_14_groups(self, saude_client: SaudeClient):
        groups = await saude_client.list_groups()
        assert len(groups) == 14
        names = {g.name for g in groups}
        assert "arboviroses" in names
        assert "saude-indigena" in names
        assert "vacinacao" in names


class TestListTags:
    @pytest.mark.asyncio
    async def test_returns_tags(self, saude_client: SaudeClient):
        tags = await saude_client.list_tags()
        assert len(tags) > 0


class TestDownloadDataset:
    @pytest.mark.asyncio
    async def test_writes_csv_files(self, saude_client: SaudeClient, tmp_path):
        dest = tmp_path / "downloads"
        paths = await saude_client.download_dataset(
            "arboviroses-dengue", dest_dir=dest, fmt="CSV"
        )
        assert all(p.exists() for p in paths)
        assert all(p.stat().st_size > 0 for p in paths)


class TestDownloadResource:
    @pytest.mark.asyncio
    async def test_writes_single_csv(self, saude_client: SaudeClient, tmp_path):
        package = await saude_client.fetch_dataset("arboviroses-dengue")
        target = next(r for r in package.resources if r.format == "CSV")
        path = await saude_client.download_resource(
            "arboviroses-dengue",
            resource_id=target.id,
            dest_dir=tmp_path,
        )
        assert path.exists()
        assert path.stat().st_size > 0


class TestCaching:
    @pytest.mark.asyncio
    async def test_cached_build_id_is_reused(
        self, saude_client: SaudeClient, tmp_path
    ):
        await saude_client.list_groups()
        # Second call should reuse the cached buildId
        await saude_client.list_groups()
        assert (tmp_path / "build_id.json").exists()
