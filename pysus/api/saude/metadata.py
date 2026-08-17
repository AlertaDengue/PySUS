"""Metadata extractors for the Saude (dadosabertos.saude.gov.br) client.

Consumes the Stage-1 models (:class:`~pysus.api.saude.resources
.CKANPackage`, :class:`~pysus.api.saude.resources.Resource`,
:class:`~pysus.api.saude.resources.GroupRef`) and produces
:class:`~pysus.api.metadata.models.MetadataBag` instances.

The CKAN package is the richest metadata source available for the
Saude origin: title, notes, license, organization, contact,
periodicity, tags and themes all map into the bag.
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
    StructureFacet,
    TemporalFacet,
)

# ----------------------------------------------------------------------
# Dataset
# ----------------------------------------------------------------------


class SaudeDatasetExtractor(MetadataExtractor):
    """Build a dataset-level bag from a CKAN package."""

    origin = "saude"

    def supported_facets(self) -> set[str]:
        return {
            "identity",
            "description",
            "temporal",
            "provenance",
            "structure",
            "access",
        }

    def _extract(self, obj: Any) -> MetadataBag:
        package = obj

        organization = ""
        if package.organization is not None:
            organization = (
                package.organization.display_name
                or package.organization.name
                or ""
            )

        return MetadataBag(
            identity=IdentityFacet(
                name=package.name,
                slug=package.name,
                cross_origin_id=package.id,
            ),
            description=DescriptionFacet(
                title=package.title,
                long_name=package.title,
                description=package.notes or "",
                tags=[
                    (tag.display_name or tag.name)
                    for tag in package.tags
                    if (tag.display_name or tag.name)
                ],
                themes=[
                    (group.display_name or group.name)
                    for group in package.groups
                    if (group.display_name or group.name)
                ],
            ),
            temporal=TemporalFacet(
                created=package.metadata_created,
                modified=package.metadata_modified,
                periodicity=package.periodicity,
            ),
            provenance=ProvenanceFacet(
                origin=self.origin,
                organization=organization,
                author=package.author or "",
                maintainer=package.maintainer or "",
                contact=package.contact or "",
                license=package.license_title or "",
                license_id=package.license_id or "",
                source_url=package.url or "",
            ),
            structure=StructureFacet(file_count=package.num_resources),
            access=AccessFacet(
                policy=f"{package.state or 'active'}/"
                f"{'private' if package.private else 'public'}/"
                f"{'open' if package.isopen else 'closed'}"
            ),
        )


# ----------------------------------------------------------------------
# Group (theme)
# ----------------------------------------------------------------------


class SaudeGroupExtractor(MetadataExtractor):
    """Build a group-level bag from a CKAN ``GroupRef``."""

    origin = "saude"

    def supported_facets(self) -> set[str]:
        return {"identity", "description", "provenance"}

    def _extract(self, obj: Any) -> MetadataBag:
        group = obj
        display = group.display_name or group.name or ""
        return MetadataBag(
            identity=IdentityFacet(name=group.name, slug=group.name),
            description=DescriptionFacet(
                title=display,
                themes=[group.name],
            ),
            provenance=ProvenanceFacet(origin=self.origin),
        )


# ----------------------------------------------------------------------
# File (resource)
# ----------------------------------------------------------------------


class SaudeFileExtractor(MetadataExtractor):
    """Build a file-level bag from a CKAN ``Resource``."""

    origin = "saude"

    def supported_facets(self) -> set[str]:
        return {
            "identity",
            "description",
            "temporal",
            "structure",
            "access",
            "quality",
            "provenance",
        }

    def _extract(self, obj: Any) -> MetadataBag:
        resource = obj
        return MetadataBag(
            identity=IdentityFacet(
                name=resource.name,
                cross_origin_id=resource.id,
            ),
            description=DescriptionFacet(
                description=resource.description or "",
            ),
            temporal=TemporalFacet(
                created=resource.created,
                modified=(resource.last_modified or resource.metadata_modified),
            ),
            structure=StructureFacet(format=resource.format or ""),
            access=AccessFacet(
                url=resource.url,
                format=resource.format or "",
                size_bytes=resource.size or 0,
                mime_type=resource.mimetype or "",
                download_strategy="http-stream",
            ),
            quality=QualityFacet(
                integrity_verified=bool(resource.hash),
            ),
            provenance=ProvenanceFacet(origin=self.origin),
            raw={
                "position": resource.position,
                "hash": resource.hash or "",
                "state": resource.state or "",
                "resource_type": resource.resource_type or "",
            },
        )


__all__ = [
    "SaudeDatasetExtractor",
    "SaudeFileExtractor",
    "SaudeGroupExtractor",
]
