"""Metadata data models: facets, bags and merge logic.

The metadata layer is origin-agnostic. Every client (FTP, DadosGov,
DuckLake, Saude) extracts metadata from a different source, but the
shape that flows through PySUS is one: a :class:`MetadataBag` with
eight typed facets (identity, description, temporal, spatial,
provenance, structure, access, quality) plus a ``raw`` dict that
preserves unmapped fields.

The legacy dataclasses (:class:`Dataset`, :class:`DatasetGroup`,
:class:`FileMeta`, :class:`File`, :class:`Column`) remain in this
module for backwards compatibility; new code should use
:class:`MetadataBag`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pysus.api.types import ColumnType, Origin

# ----------------------------------------------------------------------
# Legacy dataclasses (kept for backwards compatibility)
# ----------------------------------------------------------------------


@dataclass
class Dataset:
    """Legacy dataset metadata record."""

    name: str
    long_name: str
    description: str


@dataclass
class DatasetGroup:
    """Legacy group metadata record."""

    name: str
    long_name: str
    description: str


@dataclass
class FileMeta:
    """Legacy file metadata record."""

    name: str
    path: str
    size: int
    state: str | None = None
    uf: str | None = None
    year: int | None = None
    month: int | None = None
    origin_path: str | None = None
    origin_size: int | None = None


@dataclass
class File:
    """Legacy file record."""

    origin: Origin
    dataset: Dataset | None = None
    group: DatasetGroup | None = None
    columns: list[Column] = field(default_factory=list)
    _meta: FileMeta | None = None


@dataclass
class Column:
    """A column definition: name, description and dtype."""

    name: str
    description: str
    dtype: ColumnType

    @classmethod
    def from_schema(
        cls, name: str, dtype: ColumnType, description: str = ""
    ) -> Column:
        """Create a Column with a description provided from the database."""
        return cls(
            name=name,
            description=description,
            dtype=dtype,
        )

    def to_dict(self) -> dict[str, str]:
        """Serialize the column."""
        return {
            "name": self.name,
            "description": self.description,
            "dtype": self.dtype,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> Column:
        """Rehydrate a column from a serialized dict."""
        return cls(
            name=data["name"],
            description=data.get("description", ""),
            dtype=data["dtype"],
        )


# ----------------------------------------------------------------------
# Facets
# ----------------------------------------------------------------------

#: Geographic scope levels, from least to most specific. Used to merge
#: spatial facets (never widen coverage).
SCOPE_RANK = {
    "national": 0,
    "regional": 1,
    "state": 2,
    "municipal": 3,
    "local": 4,
}

#: Origin precedence for descriptive metadata (titles, descriptions).
#: Saude is the curated source; DuckLake is a mirror.
DESCRIPTIVE_PRECEDENCE = ("saude", "dadosgov", "ftp", "ducklake")

#: Origin precedence for content/structure metadata. DuckLake holds
#: the authoritative parquet schema and row counts.
STRUCTURE_PRECEDENCE = ("ducklake", "saude", "dadosgov", "ftp")

#: Origin precedence for temporal.modified (upload time accuracy).
MODIFIED_PRECEDENCE = ("ducklake", "ftp", "dadosgov", "saude")


@dataclass
class IdentityFacet:
    """Names and identifiers of the entity.

    ``cross_origin_id`` is the shared CKAN UUID that links the same
    dataset across the dados.gov.br and dadosabertos.saude.gov.br
    portals.
    """

    name: str = ""
    slug: str = ""
    aliases: list[str] = field(default_factory=list)
    cross_origin_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "slug": self.slug,
            "aliases": list(self.aliases),
            "cross_origin_id": self.cross_origin_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> IdentityFacet:
        return cls(
            name=data.get("name", ""),
            slug=data.get("slug", ""),
            aliases=list(data.get("aliases", [])),
            cross_origin_id=data.get("cross_origin_id", ""),
        )


@dataclass
class DescriptionFacet:
    """Human-readable title, description, tags and themes."""

    title: str = ""
    long_name: str = ""
    description: str = ""
    tags: list[str] = field(default_factory=list)
    themes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "long_name": self.long_name,
            "description": self.description,
            "tags": list(self.tags),
            "themes": list(self.themes),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DescriptionFacet:
        return cls(
            title=data.get("title", ""),
            long_name=data.get("long_name", ""),
            description=data.get("description", ""),
            tags=list(data.get("tags", [])),
            themes=list(data.get("themes", [])),
        )


@dataclass
class TemporalFacet:
    """Temporal coverage and freshness."""

    created: datetime | None = None
    modified: datetime | None = None
    periodicity: str | None = None
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    year: int | None = None
    month: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "created": self.created.isoformat() if self.created else None,
            "modified": self.modified.isoformat() if self.modified else None,
            "periodicity": self.periodicity,
            "valid_from": (
                self.valid_from.isoformat() if self.valid_from else None
            ),
            "valid_to": (self.valid_to.isoformat() if self.valid_to else None),
            "year": self.year,
            "month": self.month,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TemporalFacet:
        def _parse(value: Any) -> datetime | None:
            if not value:
                return None
            try:
                return datetime.fromisoformat(str(value))
            except ValueError:
                return None

        return cls(
            created=_parse(data.get("created")),
            modified=_parse(data.get("modified")),
            periodicity=data.get("periodicity"),
            valid_from=_parse(data.get("valid_from")),
            valid_to=_parse(data.get("valid_to")),
            year=data.get("year"),
            month=data.get("month"),
        )


@dataclass
class SpatialFacet:
    """Geographic coverage."""

    geographic_scope: str = "national"
    ufs: list[str] = field(default_factory=list)
    municipalities: list[str] = field(default_factory=list)
    state: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "geographic_scope": self.geographic_scope,
            "ufs": list(self.ufs),
            "municipalities": list(self.municipalities),
            "state": self.state,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SpatialFacet:
        return cls(
            geographic_scope=data.get("geographic_scope", "national"),
            ufs=list(data.get("ufs", [])),
            municipalities=list(data.get("municipalities", [])),
            state=data.get("state"),
        )


@dataclass
class ProvenanceFacet:
    """Where the data came from and how it may be used."""

    origin: str = ""
    organization: str = ""
    author: str = ""
    maintainer: str = ""
    contact: str = ""
    license: str = ""
    license_id: str = ""
    attribution: str = ""
    source_url: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "origin": self.origin,
            "organization": self.organization,
            "author": self.author,
            "maintainer": self.maintainer,
            "contact": self.contact,
            "license": self.license,
            "license_id": self.license_id,
            "attribution": self.attribution,
            "source_url": self.source_url,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProvenanceFacet:
        return cls(
            origin=data.get("origin", ""),
            organization=data.get("organization", ""),
            author=data.get("author", ""),
            maintainer=data.get("maintainer", ""),
            contact=data.get("contact", ""),
            license=data.get("license", ""),
            license_id=data.get("license_id", ""),
            attribution=data.get("attribution", ""),
            source_url=data.get("source_url", ""),
        )


@dataclass
class StructureFacet:
    """Schema and volume."""

    columns: list[Column] = field(default_factory=list)
    row_count: int = 0
    file_count: int = 0
    format: str = ""
    schema_fingerprint: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "columns": [c.to_dict() for c in self.columns],
            "row_count": self.row_count,
            "file_count": self.file_count,
            "format": self.format,
            "schema_fingerprint": self.schema_fingerprint,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StructureFacet:
        return cls(
            columns=[Column.from_dict(c) for c in data.get("columns", [])],
            row_count=int(data.get("row_count", 0) or 0),
            file_count=int(data.get("file_count", 0) or 0),
            format=data.get("format", ""),
            schema_fingerprint=data.get("schema_fingerprint", ""),
        )


@dataclass
class AccessFacet:
    """How to reach the data."""

    url: str = ""
    format: str = ""
    size_bytes: int = 0
    download_strategy: str = ""
    requires_auth: bool = False
    policy: str = ""
    mime_type: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "format": self.format,
            "size_bytes": self.size_bytes,
            "download_strategy": self.download_strategy,
            "requires_auth": self.requires_auth,
            "policy": self.policy,
            "mime_type": self.mime_type,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AccessFacet:
        return cls(
            url=data.get("url", ""),
            format=data.get("format", ""),
            size_bytes=int(data.get("size_bytes", 0) or 0),
            download_strategy=data.get("download_strategy", ""),
            requires_auth=bool(data.get("requires_auth", False)),
            policy=data.get("policy", ""),
            mime_type=data.get("mime_type", ""),
        )


@dataclass
class QualityFacet:
    """Integrity and completeness signals."""

    freshness_score: float | None = None
    integrity_verified: bool = False
    content_fingerprint: str = ""
    completeness_pct: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "freshness_score": self.freshness_score,
            "integrity_verified": self.integrity_verified,
            "content_fingerprint": self.content_fingerprint,
            "completeness_pct": self.completeness_pct,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> QualityFacet:
        return cls(
            freshness_score=data.get("freshness_score"),
            integrity_verified=bool(data.get("integrity_verified", False)),
            content_fingerprint=data.get("content_fingerprint", ""),
            completeness_pct=data.get("completeness_pct"),
        )


# ----------------------------------------------------------------------
# MetadataBag
# ----------------------------------------------------------------------


@dataclass
class MetadataBag:
    """The canonical metadata container.

    One bag per entity (client, dataset, group, file). Every facet has
    a documented merge rule (see :func:`merge_bags`) so bags from
    different origins can be combined without losing information.
    """

    identity: IdentityFacet = field(default_factory=IdentityFacet)
    description: DescriptionFacet = field(default_factory=DescriptionFacet)
    temporal: TemporalFacet = field(default_factory=TemporalFacet)
    spatial: SpatialFacet = field(default_factory=SpatialFacet)
    provenance: ProvenanceFacet = field(default_factory=ProvenanceFacet)
    structure: StructureFacet = field(default_factory=StructureFacet)
    access: AccessFacet = field(default_factory=AccessFacet)
    quality: QualityFacet = field(default_factory=QualityFacet)
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the bag to JSON-compatible dicts."""
        return {
            "identity": self.identity.to_dict(),
            "description": self.description.to_dict(),
            "temporal": self.temporal.to_dict(),
            "spatial": self.spatial.to_dict(),
            "provenance": self.provenance.to_dict(),
            "structure": self.structure.to_dict(),
            "access": self.access.to_dict(),
            "quality": self.quality.to_dict(),
            "raw": self.raw,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MetadataBag:
        """Rehydrate a bag from a serialized dict."""
        return cls(
            identity=IdentityFacet.from_dict(data.get("identity", {})),
            description=DescriptionFacet.from_dict(data.get("description", {})),
            temporal=TemporalFacet.from_dict(data.get("temporal", {})),
            spatial=SpatialFacet.from_dict(data.get("spatial", {})),
            provenance=ProvenanceFacet.from_dict(data.get("provenance", {})),
            structure=StructureFacet.from_dict(data.get("structure", {})),
            access=AccessFacet.from_dict(data.get("access", {})),
            quality=QualityFacet.from_dict(data.get("quality", {})),
            raw=dict(data.get("raw", {})),
        )

    def merge(
        self,
        other: MetadataBag,
        *,
        descriptive_precedence: tuple[str, ...] = DESCRIPTIVE_PRECEDENCE,
        structure_precedence: tuple[str, ...] = STRUCTURE_PRECEDENCE,
        modified_precedence: tuple[str, ...] = MODIFIED_PRECEDENCE,
    ) -> MetadataBag:
        """Return a new bag with ``other`` merged into ``self``.

        Per-facet rules (roadmap_saude.md §1.7):

        - identity: first non-empty field; aliases union
        - description: winner by ``descriptive_precedence``; tags union
        - temporal.created: earliest; temporal.modified: winner by
          ``modified_precedence``; year/month first non-None
        - spatial: most specific scope; ufs/municipalities union
        - provenance: first non-empty per field; license most
          permissive; origins joined with ``/``
        - structure: columns winner by ``structure_precedence``;
          row/file counts max
        - access: url/size first non-zero; ``requires_auth`` OR
        - quality: fingerprint first non-empty; integrity OR; freshness
          max
        """
        return merge_bags(
            [self, other],
            descriptive_precedence=descriptive_precedence,
            structure_precedence=structure_precedence,
            modified_precedence=modified_precedence,
        )


# ----------------------------------------------------------------------
# Merge helpers
# ----------------------------------------------------------------------


def _origin_of(bag: MetadataBag) -> str:
    return (bag.provenance.origin or "").strip().lower()


def _first(*values: str) -> str:
    for value in values:
        if value:
            return str(value)
    return ""


def _pick_by_precedence(
    bags: list[MetadataBag],
    getter,
    precedence: tuple[str, ...],
) -> MetadataBag | None:
    """Return the bag that wins for a facet, or None if all empty."""
    for origin in precedence:
        for bag in bags:
            if _origin_of(bag) == origin and getter(bag):
                return bag
    for bag in bags:  # unknown origins, keep first non-empty
        if getter(bag):
            return bag
    return None


def _is_bag_nonempty(bag: MetadataBag) -> bool:
    return _origin_of(bag) != ""


def merge_bags(
    bags: list[MetadataBag],
    *,
    descriptive_precedence: tuple[str, ...] = DESCRIPTIVE_PRECEDENCE,
    structure_precedence: tuple[str, ...] = STRUCTURE_PRECEDENCE,
    modified_precedence: tuple[str, ...] = MODIFIED_PRECEDENCE,
) -> MetadataBag:
    """Merge several bags (typically one per origin) into one.

    Bags whose ``provenance.origin`` is empty participate only through
    field-level fallbacks. All precedence tuples can be overridden.
    """
    if not bags:
        return MetadataBag()
    if len(bags) == 1:
        return bags[0]

    populated = [b for b in bags if _is_bag_nonempty(b)] or bags

    # identity — first non-empty; aliases union
    identity = IdentityFacet()
    identity.name = _first(*[b.identity.name for b in populated])
    identity.slug = _first(*[b.identity.slug for b in populated])
    identity.cross_origin_id = _first(
        *[b.identity.cross_origin_id for b in populated]
    )
    seen_aliases: set[str] = set()
    for b in populated:
        for alias in b.identity.aliases:
            if alias and alias not in seen_aliases:
                identity.aliases.append(alias)
                seen_aliases.add(alias)

    # description — winner by descriptive precedence; tags union
    winner = _pick_by_precedence(
        populated,
        lambda b: (
            b.description.title
            or b.description.long_name
            or b.description.description
        ),
        descriptive_precedence,
    )
    description = DescriptionFacet()
    if winner:
        description = DescriptionFacet(**winner.description.__dict__.copy())
        description.tags = list(winner.description.tags)
        description.themes = list(winner.description.themes)
    seen_tags: set[str] = set()
    for b in populated:
        for tag in b.description.tags:
            if tag and tag not in seen_tags:
                description.tags.append(tag)
                seen_tags.add(tag)
        for theme in b.description.themes:
            if theme and theme not in description.themes:
                description.themes.append(theme)

    # temporal — created earliest, modified by precedence, year/month
    # first non-None
    temporal = TemporalFacet()
    createds = [b.temporal.created for b in populated if b.temporal.created]
    temporal.created = min(createds) if createds else None
    mod_winner = _pick_by_precedence(
        populated,
        lambda b: b.temporal.modified is not None,
        modified_precedence,
    )
    if mod_winner:
        temporal.modified = mod_winner.temporal.modified
    for b in populated:
        if b.temporal.periodicity:
            temporal.periodicity = b.temporal.periodicity
            break
        if b.temporal.valid_from:
            temporal.valid_from = b.temporal.valid_from
            break
        if b.temporal.valid_to:
            temporal.valid_to = b.temporal.valid_to
            break
    for b in populated:
        if b.temporal.year is not None:
            temporal.year = b.temporal.year
            break
    for b in populated:
        if b.temporal.month is not None:
            temporal.month = b.temporal.month
            break

    # spatial — most specific scope; ufs/municipalities union
    spatial = SpatialFacet()
    scope_winner = max(
        populated,
        key=lambda b: SCOPE_RANK.get(b.spatial.geographic_scope, 0),
    )
    spatial.geographic_scope = scope_winner.spatial.geographic_scope
    for b in populated:
        for uf in b.spatial.ufs:
            if uf and uf not in spatial.ufs:
                spatial.ufs.append(uf)
        for mun in b.spatial.municipalities:
            if mun and mun not in spatial.municipalities:
                spatial.municipalities.append(mun)
    for b in populated:
        if b.spatial.state:
            spatial.state = b.spatial.state
            break

    # provenance — first non-empty; license most permissive
    provenance = ProvenanceFacet()
    provenance.organization = _first(
        *[b.provenance.organization for b in populated]
    )
    provenance.author = _first(*[b.provenance.author for b in populated])
    provenance.maintainer = _first(
        *[b.provenance.maintainer for b in populated]
    )
    provenance.contact = _first(*[b.provenance.contact for b in populated])
    provenance.attribution = _first(
        *[b.provenance.attribution for b in populated]
    )
    provenance.source_url = _first(
        *[b.provenance.source_url for b in populated]
    )
    provenance.license = _pick_license(populated)
    provenance.license_id = _pick_license_id(populated)
    origins = []
    for b in populated:
        if b.provenance.origin and b.provenance.origin not in origins:
            origins.append(b.provenance.origin)
    provenance.origin = "/".join(origins)

    # structure — columns by structure precedence, counts max
    col_winner = _pick_by_precedence(
        populated,
        lambda b: bool(b.structure.columns),
        structure_precedence,
    )
    structure = StructureFacet()
    if col_winner:
        structure.columns = list(col_winner.structure.columns)
    structure.row_count = max(
        (b.structure.row_count for b in populated), default=0
    )
    structure.file_count = max(
        (b.structure.file_count for b in populated), default=0
    )
    structure.format = _first(*[b.structure.format for b in populated])
    structure.schema_fingerprint = _first(
        *[b.structure.schema_fingerprint for b in populated]
    )

    # access — url first non-empty, size first non-zero, auth OR
    access = AccessFacet()
    access.url = _first(*[b.access.url for b in populated])
    access.format = _first(*[b.access.format for b in populated])
    for b in populated:
        if b.access.size_bytes:
            access.size_bytes = b.access.size_bytes
            break
    access.download_strategy = _first(
        *[b.access.download_strategy for b in populated]
    )
    access.requires_auth = any(b.access.requires_auth for b in populated)
    access.policy = _first(*[b.access.policy for b in populated])
    access.mime_type = _first(*[b.access.mime_type for b in populated])

    # quality — fingerprint first non-empty, integrity OR, freshness max
    quality = QualityFacet()
    quality.content_fingerprint = _first(
        *[b.quality.content_fingerprint for b in populated]
    )
    quality.integrity_verified = any(
        b.quality.integrity_verified for b in populated
    )
    scores = [
        b.quality.freshness_score
        for b in populated
        if b.quality.freshness_score is not None
    ]
    quality.freshness_score = max(scores) if scores else None
    pcts = [
        b.quality.completeness_pct
        for b in populated
        if b.quality.completeness_pct is not None
    ]
    quality.completeness_pct = max(pcts) if pcts else None

    # raw — merged dicts (later bags win on key collisions)
    raw: dict[str, Any] = {}
    for b in populated:
        raw.update(b.raw)

    return MetadataBag(
        identity=identity,
        description=description,
        temporal=temporal,
        spatial=spatial,
        provenance=provenance,
        structure=structure,
        access=access,
        quality=quality,
        raw=raw,
    )


def _pick_license(bags: list[MetadataBag]) -> str:
    """Pick the most permissive license title (CC-BY > CC-BY-SA > custom)."""
    winner = _first(*[b.provenance.license for b in bags])
    for b in bags:
        lic = b.provenance.license.lower()
        if "atribuição" in lic and "sem derivações" not in lic:
            return b.provenance.license
    return winner


def _pick_license_id(bags: list[MetadataBag]) -> str:
    """Pick the most permissive license id."""
    ids = [b.provenance.license_id for b in bags if b.provenance.license_id]
    for preferred in ("cc-by", "cc-by-sa", "cc0"):
        if preferred in ids:
            return preferred
    return ids[0] if ids else ""


__all__ = [
    "AccessFacet",
    "Column",
    "Dataset",
    "DatasetGroup",
    "DESCRIPTIVE_PRECEDENCE",
    "DescriptionFacet",
    "File",
    "FileMeta",
    "IdentityFacet",
    "MetadataBag",
    "MODIFIED_PRECEDENCE",
    "ProvenanceFacet",
    "QualityFacet",
    "SCOPE_RANK",
    "SpatialFacet",
    "STRUCTURE_PRECEDENCE",
    "StructureFacet",
    "TemporalFacet",
    "merge_bags",
]
