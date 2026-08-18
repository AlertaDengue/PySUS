"""Tests for the Saude remote models (Dataset/Group/File)."""

from __future__ import annotations

import pytest
from pysus.api.metadata.models import MetadataBag
from pysus.api.saude.client import SaudeClient
from pysus.api.saude.databases import SPECS_BY_NAME
from pysus.api.saude.models import SaudeDataset, SaudeFile, SaudeGroup
from pysus.api.saude.resources import CatalogEntry, CKANPackage


@pytest.fixture
def catalog_entry() -> CatalogEntry:
    from pysus.api.saude.resources import GroupRef, TagRef

    return CatalogEntry(
        name="arboviroses-dengue",
        title="Sinan/Dengue",
        notes="Notificações de dengue.",
        formats=["PDF", "CSV", "JSON", "XML"],
        groups=[GroupRef(name="arboviroses", display_name="Arboviroses")],
        tags=[TagRef(name="dengue", display_name="Dengue")],
    )


@pytest.fixture
def saude_dataset(saude_client: SaudeClient) -> SaudeDataset:
    return SaudeDataset(spec=SPECS_BY_NAME["ARBOVIROSES"], client=saude_client)


class TestSaudeDataset:
    def test_name_from_spec(self, saude_dataset):
        assert saude_dataset.name == "ARBOVIROSES"
        assert saude_dataset.long_name == "Arboviroses"

    def test_endpoints_from_spec(self, saude_dataset):
        assert len(saude_dataset.endpoints) == 5
        assert "/arboviroses/dengue" in saude_dataset.endpoints

    def test_metadata_from_spec(self, saude_dataset):
        bag = saude_dataset.metadata
        assert isinstance(bag, MetadataBag)
        assert bag.identity.name == "ARBOVIROSES"
        assert bag.provenance.origin == "saude"
        assert bag.description.title == "Arboviroses"

    @pytest.mark.asyncio
    async def test_content_returns_groups(self, saude_dataset):
        # catalog filtered by the spec's ckan_group against the mock
        content = await saude_dataset.content
        assert len(content) >= 1
        names = [g.name for g in content]
        assert "arboviroses-dengue" in names


class TestSaudeGroup:
    def test_properties(self, saude_dataset, catalog_entry):
        group = SaudeGroup(entry=catalog_entry, dataset=saude_dataset)
        assert group.name == "arboviroses-dengue"
        assert group.long_name == "Sinan/Dengue"
        assert group.description.startswith("Notificações")

    def test_metadata(self, saude_dataset, catalog_entry):
        group = SaudeGroup(entry=catalog_entry, dataset=saude_dataset)
        bag = group.metadata
        assert bag.identity.name == "arboviroses-dengue"
        assert bag.description.title == "Sinan/Dengue"
        assert bag.provenance.origin == "saude"

    @pytest.mark.asyncio
    async def test_files_from_package(self, saude_dataset, catalog_entry):
        group = SaudeGroup(entry=catalog_entry, dataset=saude_dataset)
        files = await group.files
        formats = {f.record.format for f in files}
        assert "API" not in formats
        assert "CSV" in formats
        assert all(isinstance(f, SaudeFile) for f in files)

    @pytest.mark.asyncio
    async def test_package_cached(self, saude_dataset, catalog_entry):
        group = SaudeGroup(entry=catalog_entry, dataset=saude_dataset)
        first = await group.fetch_package()
        second = await group.fetch_package()
        assert isinstance(first, CKANPackage)
        assert first is second


class TestSaudeFile:
    @pytest.mark.asyncio
    async def test_file_from_package(self, saude_dataset, catalog_entry):
        group = SaudeGroup(entry=catalog_entry, dataset=saude_dataset)
        files = await group.files
        csv_file = next(f for f in files if f.record.format == "CSV")
        assert csv_file.extension == ".csv"
        assert csv_file.year is not None
        assert csv_file.state is None
        assert csv_file.month is None

    def test_metadata_from_resource(self, saude_dataset, catalog_entry):
        from pysus.api.saude.resources import Resource

        resource = Resource(
            id="abc",
            name="Dengue - 2024",
            format="CSV",
            url="https://example.com/x.csv.zip",
        )
        file = SaudeFile(
            record=resource,
            dataset=saude_dataset,
            group=SaudeGroup(entry=catalog_entry, dataset=saude_dataset),
            path=resource.url,
        )
        bag = file.metadata
        assert bag.identity.name == "Dengue - 2024"
        assert bag.access.format == "CSV"
        assert bag.provenance.origin == "saude"

    @pytest.mark.asyncio
    async def test_download_writes_file(
        self, saude_dataset, catalog_entry, tmp_path
    ):
        group = SaudeGroup(entry=catalog_entry, dataset=saude_dataset)
        files = await group.files
        csv_file = next(f for f in files if f.record.format == "CSV")
        path = await csv_file._download(output=tmp_path / "out.csv.zip")
        assert path.exists()
        assert path.stat().st_size > 0


class TestSourceDifferentiation:
    """Same logical dataset on another source keeps its own declaration."""

    def test_cnes_is_declared_as_saude_source(self):
        # CNES exists on DadosGov and FTP too — the Saude spec is a
        # separate, source-scoped declaration, not a reference to them.
        spec = SPECS_BY_NAME["CNES"]
        assert spec.ckan_group is None
        assert spec.slug_patterns == ("cnes",)
        assert len(spec.endpoints) == 4

    def test_vacinacao_overlaps_pni(self):
        # PNI on DadosGov ↔ VACINACAO theme on Saude: different
        # declarations, linked later via identity.cross_origin_id.
        spec = SPECS_BY_NAME["VACINACAO"]
        assert any("doses-aplicadas-pni" in e for e in spec.endpoints)
