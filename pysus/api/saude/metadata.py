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
    """Build a dataset-level bag from a CKAN package or a theme spec.

    Two input shapes are accepted:

    - a :class:`~pysus.api.saude.resources.CKANPackage` (a concrete
      catalog dataset) — full facets incl. license, periodicity and
      the cross-origin CKAN UUID;
    - a spec-backed :class:`~pysus.api.saude.models.SaudeDataset`
      (a theme grouping several packages) — identity/description only,
      since a theme is not a single CKAN record.
    """

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
        if hasattr(obj, "metadata_created"):
            return self._from_package(obj)
        return self._from_spec(obj)

    def _from_package(self, package: Any) -> MetadataBag:
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

    def _from_spec(self, dataset: Any) -> MetadataBag:
        spec = dataset.spec
        return MetadataBag(
            identity=IdentityFacet(
                name=spec.name,
                slug=spec.name.lower(),
                aliases=list(spec.demas_tags),
            ),
            description=DescriptionFacet(
                title=spec.long_name,
                long_name=spec.long_name,
                description=spec.description,
                themes=[
                    spec.ckan_group.replace("-", " ") if spec.ckan_group else ""
                ],
            ),
            provenance=ProvenanceFacet(
                origin=self.origin,
                organization="Ministério da Saúde",
            ),
            structure=StructureFacet(
                file_count=len(spec.endpoints),
            ),
            access=AccessFacet(policy="active/public/open"),
        )


# ----------------------------------------------------------------------
# Group (theme)
# ----------------------------------------------------------------------


class SaudeGroupExtractor(MetadataExtractor):
    """Build a group-level bag from a ``GroupRef`` or a ``CatalogEntry``."""

    origin = "saude"

    def supported_facets(self) -> set[str]:
        return {"identity", "description", "provenance"}

    def _extract(self, obj: Any) -> MetadataBag:
        group = obj
        # SaudeGroup wraps a CatalogEntry
        entry = getattr(group, "entry", None)
        if entry is not None:
            return MetadataBag(
                identity=IdentityFacet(name=entry.name, slug=entry.name),
                description=DescriptionFacet(
                    title=entry.title,
                    long_name=entry.title,
                    description=entry.notes or "",
                    tags=[tag.name for tag in entry.tags],
                    themes=[g.name for g in entry.groups],
                ),
                provenance=ProvenanceFacet(origin=self.origin),
            )
        # GroupRef (theme) — has display_name
        if hasattr(group, "display_name"):
            display = group.display_name or group.name or ""
            return MetadataBag(
                identity=IdentityFacet(name=group.name, slug=group.name),
                description=DescriptionFacet(
                    title=display,
                    themes=[group.name],
                ),
                provenance=ProvenanceFacet(origin=self.origin),
            )
        # CatalogEntry — has title/notes
        return MetadataBag(
            identity=IdentityFacet(name=group.name, slug=group.name),
            description=DescriptionFacet(
                title=group.title,
                long_name=group.title,
                description=group.notes or "",
                tags=[tag.name for tag in group.tags],
                themes=[g.name for g in group.groups],
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
        # SaudeFile wraps a Resource in ``record``
        resource = getattr(obj, "record", None)
        if resource is None:
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
