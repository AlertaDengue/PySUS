"""Tests for the per-client metadata extractors."""

from __future__ import annotations

import json
import pathlib
from datetime import datetime

import pytest
from pysus.api.dadosgov.metadata import (
    DadosGovDatasetExtractor,
    DadosGovFileExtractor,
    DadosGovGroupExtractor,
)
from pysus.api.ducklake.metadata import (
    DuckLakeDatasetExtractor,
    DuckLakeFileExtractor,
    DuckLakeGroupExtractor,
)
from pysus.api.ftp.metadata import (
    FtpDatasetExtractor,
    FtpFileExtractor,
    FtpGroupExtractor,
)
from pysus.api.metadata.models import MetadataBag
from pysus.api.saude.metadata import (
    SaudeDatasetExtractor,
    SaudeFileExtractor,
    SaudeGroupExtractor,
)

_SAUDE_FIXTURES = pathlib.Path(__file__).parent.parent / "saude" / "fixtures"


@pytest.fixture(scope="module")
def saude_dataset_page_props() -> dict:
    payload = json.loads(
        (_SAUDE_FIXTURES / "dataset_arboviroses-dengue.json").read_text()
    )
    return payload["pageProps"]


class _Stub:
    """Minimal stand-in for the concrete client model classes."""

    def __init__(self, **attrs):
        self.__dict__.update(attrs)


class TestSaudeExtractors:
    def test_dataset_extractor(self, saude_dataset_page_props):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        bag = SaudeDatasetExtractor().extract(package)
        assert isinstance(bag, MetadataBag)
        assert bag.provenance.origin == "saude"
        assert bag.identity.name == "arboviroses-dengue"
        assert bag.identity.cross_origin_id == package.id
        assert bag.description.title == "Sinan/Dengue"
        assert bag.description.themes == ["Arboviroses"]
        assert bag.temporal.periodicity == "Semanal"
        assert bag.provenance.contact == "arboviroses@saude.gov.br"
        assert bag.provenance.license_id == "cc-by"
        assert bag.structure.file_count == 83

    def test_file_extractor(self, saude_dataset_page_props):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        resource = next(r for r in package.resources if r.format == "CSV")
        bag = SaudeFileExtractor().extract(resource)
        assert bag.provenance.origin == "saude"
        assert bag.identity.name == resource.name
        assert bag.access.format == "CSV"
        assert bag.access.url == resource.url
        assert "position" in bag.raw

    def test_group_extractor(self):
        from pysus.api.saude.resources import GroupRef

        group = GroupRef(name="arboviroses", display_name="Arboviroses")
        bag = SaudeGroupExtractor().extract(group)
        assert bag.provenance.origin == "saude"
        assert bag.identity.name == "arboviroses"
        assert bag.description.title == "Arboviroses"


class TestDadosGovExtractors:
    def test_file_extractor(self):
        file = _Stub(
            basename="DENGBR25.csv.zip",
            path="/x/DENGBR25.csv.zip",
            extension=".zip",
            size=1234,
            modify=datetime(2026, 1, 1),
            year=2025,
            month=3,
            state=None,
        )
        bag = DadosGovFileExtractor().extract(file)
        assert bag.provenance.origin == "dadosgov"
        assert bag.identity.name == "DENGBR25.csv.zip"
        assert bag.temporal.year == 2025
        assert bag.temporal.month == 3
        assert bag.access.size_bytes == 1234
        assert bag.access.requires_auth is True

    def test_dataset_extractor(self):
        ds = _Stub(
            name="SINAN", long_name="Sistema de Informação", description="x"
        )
        bag = DadosGovDatasetExtractor().extract(ds)
        assert bag.identity.name == "SINAN"
        assert bag.description.title == "Sistema de Informação"

    def test_group_extractor(self):
        group = _Stub(name="DENG", long_name="Dengue", description="")
        bag = DadosGovGroupExtractor().extract(group)
        assert bag.identity.name == "DENG"


class TestFtpExtractors:
    def test_file_extractor(self):
        file = _Stub(
            basename="DENGBR25.dbc",
            path="/ftp/sinan/DENGBR25.dbc",
            extension=".dbc",
            size=999,
            modify=datetime(2025, 6, 1),
            year=2025,
            month=None,
            state="BR",
        )
        bag = FtpFileExtractor().extract(file)
        assert bag.provenance.origin == "ftp"
        assert bag.structure.format == "dbc"
        assert bag.access.download_strategy == "ftp"
        assert bag.spatial.state == "BR"

    def test_dataset_extractor(self):
        ds = _Stub(name="SINAN", long_name="Sistema", description="")
        bag = FtpDatasetExtractor().extract(ds)
        assert bag.identity.name == "SINAN"

    def test_group_extractor(self):
        group = _Stub(name="DENG", long_name="Dengue", description="")
        bag = FtpGroupExtractor().extract(group)
        assert bag.identity.name == "DENG"


class TestDuckLakeExtractors:
    def test_file_extractor(self):
        record = _Stub(
            rows=1000,
            modified=datetime(2026, 2, 1),
            sha256="deadbeef",
            year=2025,
            month=None,
            state="BR",
            type="PARQUET",
        )
        file = _Stub(
            basename="DENGBR25.parquet",
            path="public/data/ftp/sinan/DENGBR25.parquet",
            record=record,
            size=2048,
            modify=datetime(2026, 2, 1),
        )
        bag = DuckLakeFileExtractor().extract(file)
        assert bag.provenance.origin == "ducklake"
        assert bag.structure.row_count == 1000
        assert bag.quality.content_fingerprint == "deadbeef"
        assert bag.quality.integrity_verified is True
        assert bag.access.download_strategy == "s3"

    def test_dataset_extractor(self):
        ds = _Stub(name="sinan", long_name="Sistema", description="")
        bag = DuckLakeDatasetExtractor().extract(ds)
        assert bag.identity.name == "sinan"

    def test_group_extractor(self):
        group = _Stub(name="DENG", long_name="Dengue", description="")
        bag = DuckLakeGroupExtractor().extract(group)
        assert bag.identity.name == "DENG"
