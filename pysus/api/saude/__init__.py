"""Async client for the OpenDataSUS portal (dadosabertos.saude.gov.br).

Stage-1 spike — catalog browsing + resource downloads only. The
DEMAS REST query path and the DuckLake sync engine integration ship
in later stages (see ``roadmap_saude.md``).
"""

from .client import SaudeClient
from .errors import (
    BuildIdMissing,
    DatasetNotFound,
    NoUsableBuildId,
    PortalChanged,
    ResourceNotFound,
    SaudeError,
)
from .resources import (
    CatalogEntry,
    CatalogPage,
    CKANPackage,
    Extra,
    GroupRef,
    Organization,
    Resource,
    TagRef,
)

__all__ = [
    "BuildIdMissing",
    "CatalogEntry",
    "CatalogPage",
    "CKANPackage",
    "DatasetNotFound",
    "Extra",
    "GroupRef",
    "NoUsableBuildId",
    "Organization",
    "PortalChanged",
    "Resource",
    "ResourceNotFound",
    "SaudeClient",
    "SaudeError",
    "TagRef",
]
