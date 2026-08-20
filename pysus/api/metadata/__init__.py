"""Unified metadata layer: facets, bags, merge and extractors.

See ``roadmap_saude.md`` §1 for the design. Public API:

- :class:`~pysus.api.metadata.models.MetadataBag` — the canonical
  metadata container (eight facets + ``raw``).
- :func:`~pysus.api.metadata.models.merge_bags` — cross-origin merge
  with per-facet precedence.
- :class:`~pysus.api.metadata.extractors.MetadataExtractor` — the
  protocol every client implements.
"""

from .extractors import MetadataExtractor
from .models import (
    AccessFacet,
    Column,
    Dataset,
    DatasetGroup,
    DescriptionFacet,
    File,
    FileMeta,
    IdentityFacet,
    MetadataBag,
    ProvenanceFacet,
    QualityFacet,
    SpatialFacet,
    StructureFacet,
    TemporalFacet,
    merge_bags,
)

__all__ = [
    "AccessFacet",
    "Column",
    "Dataset",
    "DatasetGroup",
    "DescriptionFacet",
    "File",
    "FileMeta",
    "IdentityFacet",
    "MetadataBag",
    "MetadataExtractor",
    "ProvenanceFacet",
    "QualityFacet",
    "SpatialFacet",
    "StructureFacet",
    "TemporalFacet",
    "merge_bags",
]
