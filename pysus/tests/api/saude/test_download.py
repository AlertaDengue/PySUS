"""Tests for the resource / dataset download helpers."""

from __future__ import annotations

import httpx
import pytest
from pysus.api.saude.download import (
    download_dataset,
    download_resource,
    filename_for,
)
from pysus.api.saude.errors import ResourceNotFound
from pysus.api.saude.resources import Resource


class TestFilenameFor:
    def test_uses_resource_name(self):
        resource = Resource.model_validate(
            {
                "id": "abc",
                "name": "Dengue - 2024",
                "format": "CSV",
                "url": "https://example.com/x.csv",
            }
        )
        assert (
            filename_for(resource, "arboviroses-dengue", 0)
            == "Dengue - 2024.csv"
        )

    def test_appends_format_extension_when_missing(self):
        resource = Resource.model_validate(
            {
                "id": "abc",
                "name": "Dengue 2024",
                "format": "JSON",
                "url": "https://example.com/x",
            }
        )
        assert (
            filename_for(resource, "arboviroses-dengue", 0)
            == "Dengue 2024.json"
        )

    def test_skips_extension_when_format_is_api(self):
        resource = Resource.model_validate(
            {
                "id": "abc",
                "name": "API docs",
                "format": "API",
                "url": "https://example.com/docs",
            }
        )
        assert filename_for(resource, "x", 0) == "API docs"

    def test_strips_unsafe_characters(self):
        resource = Resource.model_validate(
            {
                "id": "abc",
                "name": "weird/name?with&chars",
                "format": "CSV",
                "url": "https://example.com/x",
            }
        )
        name = filename_for(resource, "x", 0)
        assert "/" not in name
        assert "?" not in name
        assert "&" not in name


class TestDownloadResource:
    @pytest.mark.asyncio
    async def test_downloads_single_resource(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        target = next(r for r in package.resources if r.format == "CSV")

        async with httpx.AsyncClient(transport=mocked_saude) as client:
            path = await download_resource(
                client, package, resource_id=target.id, dest_dir=tmp_path
            )
        assert path.exists()
        assert path.stat().st_size > 0

    @pytest.mark.asyncio
    async def test_skips_api_resources(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        api_resource = next(r for r in package.resources if r.format == "API")
        async with httpx.AsyncClient(transport=mocked_saude) as client:
            with pytest.raises(ResourceNotFound):
                await download_resource(
                    client,
                    package,
                    resource_id=api_resource.id,
                    dest_dir=tmp_path,
                )

    @pytest.mark.asyncio
    async def test_ambiguous_selector_raises(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        async with httpx.AsyncClient(transport=mocked_saude) as client:
            with pytest.raises(ValueError, match="exactly one"):
                await download_resource(client, package, dest_dir=tmp_path)

    @pytest.mark.asyncio
    async def test_no_match_raises_not_found(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        async with httpx.AsyncClient(transport=mocked_saude) as client:
            with pytest.raises(ResourceNotFound):
                await download_resource(
                    client,
                    package,
                    resource_id="nonexistent-id",
                    dest_dir=tmp_path,
                )


class TestDownloadDataset:
    @pytest.mark.asyncio
    async def test_downloads_only_csv_resources(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        expected_csvs = sum(1 for r in package.resources if r.format == "CSV")
        assert expected_csvs > 0

        async with httpx.AsyncClient(transport=mocked_saude) as client:
            paths = await download_dataset(
                client,
                package,
                dest_dir=tmp_path,
                fmt="CSV",
            )
        assert len(paths) == expected_csvs
        for path in paths:
            assert path.exists()
            assert path.stat().st_size > 0
