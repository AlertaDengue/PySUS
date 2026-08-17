"""Pydantic models for the CKAN package / resource payloads.

The portal serves the catalog via a Next.js data layer. Every dataset
exposes a CKAN package dict with the 30 fields documented in
``roadmap_saude.md`` §0.5.2, and each package carries a
``resources[]`` list of 19 fields. We model the subset we care about;
``model_config = ConfigDict(extra="ignore")`` keeps the client
forward-compatible with new CKAN fields.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class _FlexibleModel(BaseModel):
    """Base model that ignores unknown keys and normalises timestamps."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


def _coerce_str(value: Any) -> str:
    """Coerce ``None`` / non-str to an empty string for optional fields."""
    if value is None:
        return ""
    return str(value)


def _parse_iso(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp; return ``None`` on bad input."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


class GroupRef(_FlexibleModel):
    """Reference to a CKAN group (theme)."""

    name: str
    display_name: str | None = None


class TagRef(_FlexibleModel):
    """Reference to a CKAN tag."""

    name: str
    display_name: str | None = None


class Organization(_FlexibleModel):
    """The owning organization of a CKAN package."""

    id: str
    name: str
    title: str | None = None
    display_name: str | None = None
    description: str | None = None
    created: datetime | None = None
    approval_status: str | None = None
    state: str | None = None
    image_url: str | None = None


class Extra(_FlexibleModel):
    """A single ``extras[]`` entry — free-form key/value annotations."""

    key: str
    value: str


class Resource(_FlexibleModel):
    """A downloadable file attached to a CKAN package.

    19 fields documented in ``roadmap_saude.md`` §0.5.2. ``format`` is
    normalised to uppercase on the way in.
    """

    id: str
    name: str = ""
    description: str = ""
    format: str = ""
    url: str
    size: int | None = None
    mimetype: str | None = None
    created: datetime | None = None
    last_modified: datetime | None = None
    metadata_modified: datetime | None = None
    position: int = 0
    hash: str = ""
    cache_last_updated: datetime | None = None
    cache_url: str | None = None
    datastore_active: bool | None = None
    mimetype_inner: str | None = None
    package_id: str | None = None
    resource_type: str | None = None
    state: str | None = None
    url_type: str = ""

    @field_validator(
        "name", "description", "format", "hash", "url_type", mode="before"
    )
    @classmethod
    def _coerce_str(cls, value: Any) -> str:
        return _coerce_str(value)

    @field_validator("format", mode="before")
    @classmethod
    def _normalise_format(cls, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip().upper()

    @field_validator(
        "created",
        "last_modified",
        "metadata_modified",
        "cache_last_updated",
        mode="before",
    )
    @classmethod
    def _parse_ts(cls, value: Any) -> datetime | None:
        return _parse_iso(value)


class CKANPackage(_FlexibleModel):
    """The full CKAN package for a dataset.

    30 fields documented in ``roadmap_saude.md`` §0.5.2. ``extras[]``
    is the source of the Portuguese periodicity
    (``Frequência de atualização``) and the contact email
    (``Contato``); both are exposed as properties.
    """

    id: str
    name: str
    title: str
    notes: str = ""
    author: str | None = None
    author_email: str | None = None
    creator_user_id: str | None = None
    isopen: bool = True
    license_id: str | None = None
    license_title: str | None = None
    license_url: str | None = None
    maintainer: str | None = None
    maintainer_email: str | None = None
    metadata_created: datetime
    metadata_modified: datetime
    num_resources: int
    num_tags: int = 0
    organization: Organization | None = None
    owner_org: str | None = None
    private: bool = False
    state: str = "active"
    type: str = "dataset"
    url: str | None = None
    version: str | None = None
    extras: list[Extra] = Field(default_factory=list)
    groups: list[GroupRef] = Field(default_factory=list)
    tags: list[TagRef] = Field(default_factory=list)
    resources: list[Resource] = Field(default_factory=list)

    @field_validator("notes", mode="before")
    @classmethod
    def _coerce_notes(cls, value: Any) -> str:
        return _coerce_str(value)

    @field_validator("metadata_created", "metadata_modified", mode="before")
    @classmethod
    def _parse_ts(cls, value: Any) -> datetime:
        ts = _parse_iso(value)
        if ts is None:
            raise ValueError(f"Invalid timestamp: {value!r}")
        return ts

    @property
    def periodicity(self) -> str | None:
        """The ``Frequência de atualização`` extra, or ``None``."""
        for extra in self.extras:
            if extra.key == "Frequência de atualização":
                return extra.value
        return None

    @property
    def contact(self) -> str | None:
        """The ``Contato`` extra, or ``None``."""
        for extra in self.extras:
            if extra.key == "Contato":
                return extra.value
        return None

    @property
    def ckan_id(self) -> str:
        """Alias for ``id`` (the shared UUID across portals)."""
        return self.id


class CatalogEntry(_FlexibleModel):
    """A package projected for the catalog listing endpoint.

    The paginated ``/dataset.json`` response strips most fields and
    keeps only what the catalog UI needs (see
    ``roadmap_saude.md`` §0.5.2 for the full mapping).
    """

    name: str
    title: str
    notes: str = ""
    formats: list[str] = Field(default_factory=list)
    groups: list[GroupRef] = Field(default_factory=list)
    tags: list[TagRef] = Field(default_factory=list)

    @field_validator("notes", mode="before")
    @classmethod
    def _coerce_notes(cls, value: Any) -> str:
        return _coerce_str(value)


class CatalogPage(_FlexibleModel):
    """One page of the Next.js catalog listing response.

    CKAN uses camelCase keys; pydantic reads them via the
    ``AliasChoices`` so the snake_case Python API still works.
    """

    packages: list[CatalogEntry] = Field(default_factory=list, alias="packages")
    number_of_packages: int = Field(
        default=0, validation_alias="numberOfPackages"
    )
    page: int = Field(default=1)
    rows: int = Field(default=0)
    current_filters: dict[str, Any] = Field(
        default_factory=dict, validation_alias="currentFilters"
    )
    available_filters: dict[str, list[dict[str, str]]] = Field(
        default_factory=dict, validation_alias="availableFilters"
    )

    @field_validator("page", "rows", mode="before")
    @classmethod
    def _coerce_int(cls, value: Any) -> int:
        if value is None or value == "":
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0
