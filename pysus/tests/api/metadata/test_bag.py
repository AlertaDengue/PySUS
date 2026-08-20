"""Tests for MetadataBag round-trip and merge precedence."""

from __future__ import annotations

from datetime import datetime

from pysus.api.metadata.models import (
    AccessFacet,
    Column,
    DescriptionFacet,
    IdentityFacet,
    MetadataBag,
    ProvenanceFacet,
    QualityFacet,
    SpatialFacet,
    StructureFacet,
    TemporalFacet,
    merge_bags,
)
from pysus.api.types import VARCHAR


def _bag(origin: str, **facets) -> MetadataBag:
    bag = MetadataBag(provenance=ProvenanceFacet(origin=origin))
    for name, facet in facets.items():
        setattr(bag, name, facet)
    return bag


class TestRoundTrip:
    def test_empty_bag_roundtrip(self):
        bag = MetadataBag()
        restored = MetadataBag.from_dict(bag.to_dict())
        assert restored == bag

    def test_full_bag_roundtrip(self):
        bag = MetadataBag(
            identity=IdentityFacet(
                name="SINAN",
                slug="arboviroses-dengue",
                aliases=["DENG"],
                cross_origin_id="4d5e5d44-58a8-4d67-b8aa-4ef1e4b00a1c",
            ),
            description=DescriptionFacet(
                title="Sinan/Dengue",
                description="Notificações de dengue",
                tags=["dengue", "arbovirose"],
                themes=["Arboviroses"],
            ),
            temporal=TemporalFacet(
                created=datetime(2024, 2, 22, 19, 23, 6),
                modified=datetime(2026, 8, 16, 3, 59, 48),
                periodicity="Semanal",
                year=2025,
                month=3,
            ),
            spatial=SpatialFacet(
                geographic_scope="state",
                ufs=["RJ", "SP"],
                municipalities=["330455"],
                state="RJ",
            ),
            provenance=ProvenanceFacet(
                origin="saude",
                organization="Ministério da Saúde",
                license="Creative Commons Atribuição",
                license_id="cc-by",
                contact="arboviroses@saude.gov.br",
            ),
            structure=StructureFacet(
                columns=[Column("DT_NOTIFIC", "", VARCHAR)],
                row_count=172_855,
                file_count=83,
                format="CSV",
                schema_fingerprint="abc123",
            ),
            access=AccessFacet(
                url="https://example.com/x.csv.zip",
                format="CSV",
                size_bytes=1024,
                requires_auth=False,
                policy="active/public/open",
            ),
            quality=QualityFacet(
                content_fingerprint="deadbeef",
                integrity_verified=True,
                completeness_pct=98.5,
            ),
            raw={"position": 2},
        )
        restored = MetadataBag.from_dict(bag.to_dict())
        assert restored == bag

    def test_to_dict_is_json_compatible(self):
        import json

        bag = MetadataBag(
            temporal=TemporalFacet(
                modified=datetime(2026, 8, 16, 3, 59, 48),
            ),
            structure=StructureFacet(
                columns=[Column("A", "desc", VARCHAR)],
            ),
        )
        json.dumps(bag.to_dict())


class TestMergeIdentity:
    def test_first_non_empty_wins(self):
        a = _bag("ftp", identity=IdentityFacet(name="SINAN"))
        b = _bag("saude", identity=IdentityFacet(name="", slug="x"))
        merged = merge_bags([a, b])
        assert merged.identity.name == "SINAN"
        assert merged.identity.slug == "x"

    def test_aliases_union(self):
        a = _bag("ftp", identity=IdentityFacet(aliases=["DENG"]))
        b = _bag("dadosgov", identity=IdentityFacet(aliases=["DENG", "DENGUE"]))
        merged = merge_bags([a, b])
        assert merged.identity.aliases == ["DENG", "DENGUE"]

    def test_cross_origin_id_first_non_empty(self):
        a = _bag("ftp", identity=IdentityFacet())
        b = _bag("saude", identity=IdentityFacet(cross_origin_id="uuid-1"))
        merged = merge_bags([a, b])
        assert merged.identity.cross_origin_id == "uuid-1"


class TestMergeDescription:
    def test_saude_wins_over_ftp(self):
        a = _bag(
            "ftp",
            description=DescriptionFacet(title="SINAN", description="ftp desc"),
        )
        b = _bag(
            "saude",
            description=DescriptionFacet(
                title="Sinan/Dengue", description="saude desc"
            ),
        )
        merged = merge_bags([a, b])
        assert merged.description.title == "Sinan/Dengue"
        assert merged.description.description == "saude desc"

    def test_tags_union(self):
        a = _bag("ftp", description=DescriptionFacet(tags=["x"]))
        b = _bag("saude", description=DescriptionFacet(tags=["y"]))
        merged = merge_bags([a, b])
        assert sorted(merged.description.tags) == ["x", "y"]


class TestMergeTemporal:
    def test_created_earliest(self):
        a = _bag(
            "ftp",
            temporal=TemporalFacet(created=datetime(2024, 1, 1)),
        )
        b = _bag(
            "saude",
            temporal=TemporalFacet(created=datetime(2023, 1, 1)),
        )
        merged = merge_bags([a, b])
        assert merged.temporal.created == datetime(2023, 1, 1)

    def test_modified_ducklake_wins(self):
        a = _bag(
            "dadosgov",
            temporal=TemporalFacet(modified=datetime(2026, 1, 1)),
        )
        b = _bag(
            "ducklake",
            temporal=TemporalFacet(modified=datetime(2025, 1, 1)),
        )
        merged = merge_bags([a, b])
        # ducklake precedes dadosgov in MODIFIED_PRECEDENCE
        assert merged.temporal.modified == datetime(2025, 1, 1)

    def test_year_month_first_non_none(self):
        a = _bag("ftp", temporal=TemporalFacet(year=None, month=3))
        b = _bag("saude", temporal=TemporalFacet(year=2025, month=None))
        merged = merge_bags([a, b])
        assert merged.temporal.year == 2025
        assert merged.temporal.month == 3


class TestMergeSpatial:
    def test_most_specific_scope_wins(self):
        a = _bag("ftp", spatial=SpatialFacet(geographic_scope="national"))
        b = _bag("saude", spatial=SpatialFacet(geographic_scope="state"))
        merged = merge_bags([a, b])
        assert merged.spatial.geographic_scope == "state"

    def test_ufs_union(self):
        a = _bag("ftp", spatial=SpatialFacet(ufs=["RJ"]))
        b = _bag("dadosgov", spatial=SpatialFacet(ufs=["SP"]))
        merged = merge_bags([a, b])
        assert sorted(merged.spatial.ufs) == ["RJ", "SP"]


class TestMergeProvenance:
    def test_origins_joined(self):
        a = _bag("ftp")
        b = _bag("saude")
        merged = merge_bags([a, b])
        assert merged.provenance.origin == "ftp/saude"

    def test_license_prefers_cc_by(self):
        a = _bag(
            "ftp",
            provenance=ProvenanceFacet(
                license="Creative Commons Atribuição", license_id="cc-by"
            ),
        )
        b = _bag(
            "saude",
            provenance=ProvenanceFacet(
                license="Creative Commons Atribuição-SemDerivações",
                license_id="cc-by-nd",
            ),
        )
        merged = merge_bags([a, b])
        assert merged.provenance.license_id == "cc-by"

    def test_contact_first_non_empty(self):
        a = _bag("ftp", provenance=ProvenanceFacet(contact=""))
        b = _bag("saude", provenance=ProvenanceFacet(contact="a@b.c"))
        merged = merge_bags([a, b])
        assert merged.provenance.contact == "a@b.c"


class TestMergeStructure:
    def test_ducklake_columns_win(self):
        duck_col = Column("DT_NOTIFIC", "", VARCHAR)
        saude_col = Column("DT_NOTIFIC", "saude col", VARCHAR)
        a = _bag("saude", structure=StructureFacet(columns=[saude_col]))
        b = _bag("ducklake", structure=StructureFacet(columns=[duck_col]))
        merged = merge_bags([a, b])
        assert merged.structure.columns == [duck_col]

    def test_row_count_max(self):
        a = _bag("ftp", structure=StructureFacet(row_count=100))
        b = _bag("ducklake", structure=StructureFacet(row_count=500))
        merged = merge_bags([a, b])
        assert merged.structure.row_count == 500


class TestMergeAccess:
    def test_requires_auth_or(self):
        a = _bag("ftp", access=AccessFacet(requires_auth=False))
        b = _bag("dadosgov", access=AccessFacet(requires_auth=True))
        merged = merge_bags([a, b])
        assert merged.access.requires_auth is True

    def test_size_first_non_zero(self):
        a = _bag("ftp", access=AccessFacet(size_bytes=0))
        b = _bag("saude", access=AccessFacet(size_bytes=1024))
        merged = merge_bags([a, b])
        assert merged.access.size_bytes == 1024


class TestMergeQuality:
    def test_integrity_or(self):
        a = _bag("ftp", quality=QualityFacet(integrity_verified=False))
        b = _bag("ducklake", quality=QualityFacet(integrity_verified=True))
        merged = merge_bags([a, b])
        assert merged.quality.integrity_verified is True

    def test_fingerprint_first_non_empty(self):
        a = _bag("ftp", quality=QualityFacet(content_fingerprint=""))
        b = _bag("ducklake", quality=QualityFacet(content_fingerprint="sha"))
        merged = merge_bags([a, b])
        assert merged.quality.content_fingerprint == "sha"


class TestMergeEdgeCases:
    def test_single_bag_identity(self):
        bag = _bag("saude")
        assert merge_bags([bag]) is bag

    def test_no_bags(self):
        assert merge_bags([]) == MetadataBag()

    def test_bag_merge_method(self):
        a = _bag("ftp", description=DescriptionFacet(title="ftp"))
        b = _bag("saude", description=DescriptionFacet(title="saude"))
        merged = a.merge(b)
        assert merged.description.title == "saude"
