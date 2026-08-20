"""Tests for Saude metadata extractors."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pysus.api.saude.client import SaudeClient
from pysus.api.saude.databases import SPECS_BY_NAME
from pysus.api.saude.metadata import (
    SaudeDatasetExtractor,
    SaudeEndpointFileExtractor,
    SaudeFileExtractor,
    SaudeGroupExtractor,
)
from pysus.api.saude.resources import CatalogEntry, GroupRef, Resource, TagRef
from pysus.api.saude.rest import EndpointSpec


@pytest.fixture
def catalog_entry() -> CatalogEntry:
    return CatalogEntry(
        name="arboviroses-dengue",
        title="Sinan/Dengue",
        notes="Notificacoes de dengue.",
        formats=["PDF", "CSV", "JSON", "XML"],
        groups=[GroupRef(name="arboviroses", display_name="Arboviroses")],
        tags=[TagRef(name="dengue", display_name="Dengue")],
    )


@pytest.fixture
def saude_dataset(saude_client: SaudeClient):
    from pysus.api.saude.models import SaudeDataset

    return SaudeDataset(spec=SPECS_BY_NAME["ARBOVIROSES"], client=saude_client)


class TestSaudeDatasetExtractor:
    def test_supported_facets(self):
        ext = SaudeDatasetExtractor()
        facets = ext.supported_facets()
        assert "identity" in facets
        assert "provenance" in facets
        assert "description" in facets

    def test_from_package(self, saude_dataset_page_props):
        from pysus.api.saude.resources import CKANPackage

        pkg = CKANPackage.model_validate(saude_dataset_page_props)
        ext = SaudeDatasetExtractor()
        bag = ext._extract(pkg)
        assert bag.identity.name == pkg.name
        assert bag.identity.cross_origin_id == pkg.id

    def test_from_package_no_organization(self):
        pkg = SimpleNamespace(
            name="test",
            id="uuid-123",
            title="Test",
            notes=None,
            tags=[],
            groups=[],
            metadata_created=None,
            metadata_modified=None,
            periodicity=None,
            organization=None,
            author=None,
            maintainer=None,
            contact=None,
            license_title=None,
            license_id=None,
            url=None,
            num_resources=0,
            state="active",
            private=False,
            isopen=False,
        )
        bag = SaudeDatasetExtractor()._extract(pkg)
        assert bag.provenance.organization == ""

    def test_from_spec(self, saude_dataset):
        bag = saude_dataset.metadata
        assert bag.identity.name == "ARBOVIROSES"
        assert bag.provenance.origin == "saude"

    def test_from_spec_no_ckan_group(self):
        from pysus.api.saude.databases import DatasetSpec

        ds_spec = DatasetSpec(
            name="X",
            long_name="X",
            description="",
            ckan_group=None,
            slug_patterns=("x",),
            exclude_patterns=(),
            demas_tags=(),
            endpoints=(),
        )

        class FakeDataset:
            spec = ds_spec

        bag = SaudeDatasetExtractor()._extract(FakeDataset())
        assert bag.description.themes == [""]


class TestSaudeGroupExtractor:
    def test_supported_facets(self):
        ext = SaudeGroupExtractor()
        facets = ext.supported_facets()
        assert facets == {"identity", "description", "provenance"}

    def test_from_saude_group(self, saude_dataset, catalog_entry):
        from pysus.api.saude.models import SaudeGroup

        group = SaudeGroup(entry=catalog_entry, dataset=saude_dataset)
        bag = SaudeGroupExtractor()._extract(group)
        assert bag.identity.name == "arboviroses-dengue"

    def test_from_group_ref(self):
        ref = GroupRef(name="arboviroses", display_name="Arboviroses")
        bag = SaudeGroupExtractor()._extract(ref)
        assert bag.identity.name == "arboviroses"
        assert bag.description.title == "Arboviroses"
        assert bag.description.themes == ["arboviroses"]

    def test_from_group_ref_no_display(self):
        ref = GroupRef(name="arboviroses", display_name="")
        bag = SaudeGroupExtractor()._extract(ref)
        assert bag.description.title == "arboviroses"

    def test_from_catalog_entry_direct(self):
        entry = CatalogEntry(
            name="test",
            title="Test Title",
            notes="Some notes",
            formats=[],
            groups=[GroupRef(name="g1", display_name="G1")],
            tags=[TagRef(name="t1", display_name="T1")],
        )
        bag = SaudeGroupExtractor()._extract(entry)
        assert bag.identity.name == "test"
        assert bag.description.title == "Test Title"
        assert bag.description.tags == ["t1"]


class TestSaudeFileExtractor:
    def test_supported_facets(self):
        ext = SaudeFileExtractor()
        facets = ext.supported_facets()
        assert "quality" in facets

    def test_from_saude_file(self, saude_dataset, catalog_entry):
        from pysus.api.saude.models import SaudeFile, SaudeGroup

        group = SaudeGroup(entry=catalog_entry, dataset=saude_dataset)
        resource = Resource(
            id="r1",
            name="Dengue 2024",
            format="CSV",
            url="https://example.com/x.csv",
            size=5000,
            hash="abc123",
            mimetype="text/csv",
        )
        file = SaudeFile(
            record=resource,
            dataset=saude_dataset,
            group=group,
            path=resource.url,
        )
        bag = SaudeFileExtractor()._extract(file)
        assert bag.identity.name == "Dengue 2024"
        assert bag.access.format == "CSV"
        assert bag.quality.integrity_verified is True

    def test_from_raw_resource(self):
        resource = Resource(
            id="r1",
            name="test",
            format="JSON",
            url="https://example.com/x.json",
        )
        bag = SaudeFileExtractor()._extract(resource)
        assert bag.identity.name == "test"
        assert bag.quality.integrity_verified is False

    def test_from_resource_no_description(self):
        resource = Resource(
            id="r1",
            name="test",
            format="CSV",
            url="https://example.com/x.csv",
            description=None,
            last_modified=None,
            metadata_modified=None,
            mimetype=None,
            state=None,
            resource_type=None,
            hash=None,
        )
        bag = SaudeFileExtractor()._extract(resource)
        assert bag.description.description == ""


class TestSaudeEndpointFileExtractor:
    def test_supported_facets(self):
        ext = SaudeEndpointFileExtractor()
        facets = ext.supported_facets()
        assert facets == {
            "identity",
            "description",
            "structure",
            "access",
            "provenance",
        }

    def test_from_endpoint_spec(self):
        spec = EndpointSpec(
            path="/arboviroses/dengue",
            summary="Dengue Data",
            tag="Agravo",
        )
        bag = SaudeEndpointFileExtractor()._extract(spec)
        assert bag.identity.name == "/arboviroses/dengue"
        assert bag.structure.format == "jsonl"
        assert "arboviroses/dengue" in bag.access.url

    def test_from_wrapped_endpoint_file(self):
        spec = EndpointSpec(
            path="/test",
            summary="Test Endpoint",
            tag="Test",
        )

        class FakeEndpointFile:
            record = spec

        bag = SaudeEndpointFileExtractor()._extract(FakeEndpointFile())
        assert bag.description.title == "Test Endpoint"
