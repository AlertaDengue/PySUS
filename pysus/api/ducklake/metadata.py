"""Metadata extractors for the DuckLake (S3 catalog) client.

Consumes the client's own models (``File``, ``DuckGroup``,
``DuckDataset`` in :mod:`pysus.api.ducklake.models`) and produces
:class:`~pysus.api.metadata.models.MetadataBag` instances.

DuckLake holds the authoritative parquet artifacts: row counts, the
``sha256`` content digest and the post-ETL schema live in the catalog
rows and map into the structure and quality facets.
"""

from __future__ import annotations

from typing import Any

from pysus.api.metadata.extractors import MetadataExtractor
from pysus.api.metadata.models import (
    AccessFacet,
    DescriptionFacet,
    IdentityFacet,
    MetadataBag,
    ProvenanceFacet,
    QualityFacet,
    SpatialFacet,
    StructureFacet,
    TemporalFacet,
)


class DuckLakeDatasetExtractor(MetadataExtractor):
    """Build a dataset-level bag from a ``DuckDataset``."""

    origin = "ducklake"

    def supported_facets(self) -> set[str]:
        return {"identity", "description", "provenance"}

    def _extract(self, obj: Any) -> MetadataBag:
        dataset = obj
        return MetadataBag(
            identity=IdentityFacet(name=dataset.name),
            description=DescriptionFacet(
                title=dataset.long_name,
                long_name=dataset.long_name,
                description=getattr(dataset, "description", "") or "",
            ),
            provenance=ProvenanceFacet(origin=self.origin),
        )


class DuckLakeGroupExtractor(MetadataExtractor):
    """Build a group-level bag from a ``DuckGroup``."""

    origin = "ducklake"

    def supported_facets(self) -> set[str]:
        return {"identity", "description", "provenance"}

    def _extract(self, obj: Any) -> MetadataBag:
        group = obj
        return MetadataBag(
            identity=IdentityFacet(name=group.name),
            description=DescriptionFacet(
                title=group.long_name,
                long_name=group.long_name,
                description=getattr(group, "description", "") or "",
            ),
            provenance=ProvenanceFacet(origin=self.origin),
        )


class DuckLakeFileExtractor(MetadataExtractor):
    """Build a file-level bag from a DuckLake ``File`` (catalog row)."""

    origin = "ducklake"

    def supported_facets(self) -> set[str]:
        return {
            "identity",
            "temporal",
            "spatial",
            "structure",
            "access",
            "quality",
            "provenance",
        }

    def _extract(self, obj: Any) -> MetadataBag:
        file = obj
        record = file.record
        sha = getattr(record, "sha256", None) or ""
        modified = None
        try:
            modified = file.modify
        except (ValueError, AttributeError):
            modified = getattr(record, "modified", None)
        state = getattr(record, "state", None)
        return MetadataBag(
            identity=IdentityFacet(
                name=file.basename,
                slug=file.basename,
            ),
            temporal=TemporalFacet(
                modified=modified,
                year=getattr(record, "year", None),
                month=getattr(record, "month", None),
            ),
            spatial=SpatialFacet(state=state),
            structure=StructureFacet(
                row_count=getattr(record, "rows", 0) or 0,
                format=getattr(record, "type", None) or "parquet",
            ),
            access=AccessFacet(
                url=str(file.path),
                format="parquet",
                size_bytes=file.size or 0,
                download_strategy="s3",
            ),
            quality=QualityFacet(
                content_fingerprint=sha,
                integrity_verified=bool(sha),
            ),
            provenance=ProvenanceFacet(origin=self.origin),
        )


__all__ = [
    "DuckLakeDatasetExtractor",
    "DuckLakeFileExtractor",
    "DuckLakeGroupExtractor",
]
