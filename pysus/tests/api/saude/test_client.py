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


class TestProperties:
    def test_name(self, saude_client: SaudeClient):
        assert saude_client.name == "Saude"

    def test_long_name(self, saude_client: SaudeClient):
        assert saude_client.long_name == "Portal de Dados Abertos do SUS"

    def test_description(self, saude_client: SaudeClient):
        assert "Ministério" in saude_client.description


class TestDownload:
    @pytest.mark.asyncio
    async def test_delegates_to_file(self, saude_client: SaudeClient, tmp_path):
        from unittest.mock import AsyncMock, MagicMock

        mock_file = MagicMock()
        mock_file._download = AsyncMock(return_value=tmp_path / "out.csv")
        result = await saude_client.download(mock_file, tmp_path / "out.csv")
        mock_file._download.assert_called_once()
        assert result == tmp_path / "out.csv"


class TestEnsureBuildId:
    @pytest.mark.asyncio
    async def test_refetches_when_use_cache_false(
        self, saude_client: SaudeClient, tmp_path
    ):
        from unittest.mock import patch

        await saude_client.list_groups()
        old_id = saude_client._build_id
        assert old_id is not None

        new_id = "completely-new-build-id"

        async def fake_fetch(*args, **kwargs):
            return new_id

        with patch(
            "pysus.api.saude.client.fetch_build_id",
            side_effect=fake_fetch,
        ):
            result = await saude_client._ensure_build_id(use_cache=False)
        assert result == new_id

    @pytest.mark.asyncio
    async def test_reuses_when_use_cache_true(
        self, saude_client: SaudeClient, tmp_path
    ):
        await saude_client.list_groups()
        old_id = saude_client._build_id
        result = await saude_client._ensure_build_id(use_cache=True)
        assert result == old_id


class TestDatasets:
    @pytest.mark.asyncio
    async def test_returns_saude_datasets(self, saude_client: SaudeClient):
        from pysus.api.saude.models import SaudeDataset

        datasets = await saude_client.datasets()
        assert len(datasets) > 0
        assert all(isinstance(d, SaudeDataset) for d in datasets)
