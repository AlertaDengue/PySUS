"""Tests for the pydantic resource / package models."""

from __future__ import annotations

from pysus.api.saude.resources import (
    CatalogEntry,
    CatalogPage,
    CKANPackage,
    Extra,
    GroupRef,
    Organization,
    Resource,
    TagRef,
)


class TestResourceModel:
    def test_parses_full_resource(self, saude_dataset_page_props):
        first = saude_dataset_page_props["resources"][0]
        resource = Resource.model_validate(first)
        assert resource.id == first["id"]
        assert resource.name == first["name"]
        assert resource.format == first["format"].upper()
        assert resource.url == first["url"]
        assert resource.position == first["position"]
        assert resource.created is not None
        assert resource.last_modified is not None

    def test_normalises_format_to_uppercase(self):
        resource = Resource.model_validate(
            {
                "id": "abc",
                "name": "x",
                "format": "csv",
                "url": "https://example.com/x.csv",
            }
        )
        assert resource.format == "CSV"

    def test_ignores_unknown_keys(self):
        resource = Resource.model_validate(
            {
                "id": "abc",
                "name": "x",
                "format": "CSV",
                "url": "https://example.com/x.csv",
                "future_field_added_by_ckan": "noise",
            }
        )
        assert resource.id == "abc"

    def test_handles_null_dates(self):
        resource = Resource.model_validate(
            {
                "id": "abc",
                "name": "x",
                "format": "CSV",
                "url": "https://example.com/x.csv",
                "created": None,
                "last_modified": None,
            }
        )
        assert resource.created is None
        assert resource.last_modified is None


class TestCKANPackageModel:
    def test_parses_full_package(self, saude_dataset_page_props):
        package = CKANPackage.model_validate(saude_dataset_page_props)
        assert package.id == saude_dataset_page_props["id"]
        assert package.name == "arboviroses-dengue"
        assert package.num_resources == 83
        assert package.organization is not None
        assert package.organization.id is not None
        assert len(package.resources) == 83

    def test_periodicity_property(self, saude_dataset_page_props):
        package = CKANPackage.model_validate(saude_dataset_page_props)
        assert package.periodicity == "Semanal"

    def test_contact_property(self, saude_dataset_page_props):
        package = CKANPackage.model_validate(saude_dataset_page_props)
        assert package.contact == "arboviroses@saude.gov.br"

    def test_ckan_id_alias(self, saude_dataset_page_props):
        package = CKANPackage.model_validate(saude_dataset_page_props)
        assert package.ckan_id == package.id

    def test_organization_populated(self, saude_dataset_page_props):
        package = CKANPackage.model_validate(saude_dataset_page_props)
        assert isinstance(package.organization, Organization)

    def test_groups_have_display_name(self, saude_dataset_page_props):
        package = CKANPackage.model_validate(saude_dataset_page_props)
        assert package.groups[0].display_name is not None

    def test_periodicity_missing_returns_none(self):
        package = CKANPackage.model_validate(
            {
                "id": "abc",
                "name": "x",
                "title": "X",
                "metadata_created": "2024-01-01T00:00:00",
                "metadata_modified": "2024-01-01T00:00:00",
                "num_resources": 0,
                "extras": [],
            }
        )
        assert package.periodicity is None
        assert package.contact is None


class TestCatalogEntryModel:
    def test_parses_listing_entry(self, saude_catalog_payload):
        first = saude_catalog_payload["pageProps"]["packages"][0]
        entry = CatalogEntry.model_validate(first)
        assert entry.name == first["name"]
        assert entry.title == first["title"]
        assert "CSV" in entry.formats
        assert isinstance(entry.groups[0], GroupRef)
        assert isinstance(entry.tags[0], TagRef)


class TestCatalogPageModel:
    def test_parses_catalog_page(self, saude_catalog_payload):
        page = CatalogPage.model_validate(saude_catalog_payload["pageProps"])
        assert page.number_of_packages == 138
        assert page.page == 1
        assert page.rows == 20
        assert len(page.packages) == 20
        assert "groups" in page.available_filters
        assert "tags" in page.available_filters


class TestExtraModel:
    def test_basic(self):
        extra = Extra(key="Frequência de atualização", value="Semanal")
        assert extra.key == "Frequência de atualização"
        assert extra.value == "Semanal"
