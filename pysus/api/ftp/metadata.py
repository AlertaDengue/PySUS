"""Metadata extractors for the FTP (DATASUS) client.

Consumes the client's own models (``File``, ``Group``, ``Dataset`` in
:mod:`pysus.api.ftp.models`) and produces
:class:`~pysus.api.metadata.models.MetadataBag` instances.

The LIST-parser-derived size/modify timestamps and the per-dataset
formatter output (year/month/state/group) map into the temporal,
spatial and identity facets.
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
    SpatialFacet,
    StructureFacet,
    TemporalFacet,
)


class FtpDatasetExtractor(MetadataExtractor):
    """Build a dataset-level bag from an FTP ``Dataset``."""

    origin = "ftp"

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


class FtpGroupExtractor(MetadataExtractor):
    """Build a group-level bag from an FTP ``Group``."""

    origin = "ftp"

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


class FtpFileExtractor(MetadataExtractor):
    """Build a file-level bag from an FTP ``File``."""

    origin = "ftp"

    def supported_facets(self) -> set[str]:
        return {
            "identity",
            "temporal",
            "spatial",
            "access",
            "structure",
            "provenance",
        }

    def _extract(self, obj: Any) -> MetadataBag:
        file = obj
        modified = None
        try:
            modified = file.modify
        except (ValueError, AttributeError):
            modified = None
        state = None
        try:
            state = file.state
        except (ValueError, AttributeError):
            state = None
        return MetadataBag(
            identity=IdentityFacet(
                name=file.basename,
                slug=file.basename,
            ),
            temporal=TemporalFacet(
                modified=modified,
                year=file.year,
                month=file.month,
            ),
            spatial=SpatialFacet(state=state),
            structure=StructureFacet(
                format=(file.extension or "").lstrip("."),
            ),
            access=AccessFacet(
                url=str(file.path),
                format=(file.extension or "").lstrip("."),
                size_bytes=file.size or 0,
                download_strategy="ftp",
            ),
            provenance=ProvenanceFacet(origin=self.origin),
        )


__all__ = [
    "FtpDatasetExtractor",
    "FtpFileExtractor",
    "FtpGroupExtractor",
]
