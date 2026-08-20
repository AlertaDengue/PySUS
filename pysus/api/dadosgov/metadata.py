"""Metadata extractors for the DadosGov (dados.gov.br) client.

Consumes the client's own models (``File``, ``Group``, ``Dataset`` in
:mod:`pysus.api.dadosgov.models`) and produces
:class:`~pysus.api.metadata.models.MetadataBag` instances.

The filename-derived ``year`` / ``month`` / ``state`` metadata (from
the per-dataset formatters) maps into the temporal and spatial facets.
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


class DadosGovDatasetExtractor(MetadataExtractor):
    """Build a dataset-level bag from a DadosGov ``Dataset``."""

    origin = "dadosgov"

    def supported_facets(self) -> set[str]:
        return {"identity", "description", "provenance"}

    def _extract(self, obj: Any) -> MetadataBag:
        dataset = obj
        return MetadataBag(
            identity=IdentityFacet(
                name=dataset.name,
                slug=getattr(dataset, "slug", "") or dataset.name,
            ),
            description=DescriptionFacet(
                title=dataset.long_name,
                long_name=dataset.long_name,
                description=getattr(dataset, "description", "") or "",
            ),
            provenance=ProvenanceFacet(origin=self.origin),
        )


class DadosGovGroupExtractor(MetadataExtractor):
    """Build a group-level bag from a DadosGov ``Group``."""

    origin = "dadosgov"

    def supported_facets(self) -> set[str]:
        return {"identity", "description", "provenance"}

    def _extract(self, obj: Any) -> MetadataBag:
        group = obj
        return MetadataBag(
            identity=IdentityFacet(
                name=group.name,
                slug=getattr(group, "slug", "") or group.name,
            ),
            description=DescriptionFacet(
                title=group.long_name,
                long_name=group.long_name,
                description=getattr(group, "description", "") or "",
            ),
            provenance=ProvenanceFacet(origin=self.origin),
        )


class DadosGovFileExtractor(MetadataExtractor):
    """Build a file-level bag from a DadosGov ``File``."""

    origin = "dadosgov"

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
                download_strategy="http",
                requires_auth=True,
            ),
            provenance=ProvenanceFacet(origin=self.origin),
        )


__all__ = [
    "DadosGovDatasetExtractor",
    "DadosGovFileExtractor",
    "DadosGovGroupExtractor",
]
