"""The ``MetadataExtractor`` protocol and registry helpers.

One extractor implementation exists per (origin, entity_type) pair:

- FTP: `FtpDatasetExtractor`, `FtpGroupExtractor`, `FtpFileExtractor`
- DadosGov: `DadosGovDatasetExtractor`, `DadosGovGroupExtractor`,
  `DadosGovFileExtractor`
- DuckLake: `DuckLakeDatasetExtractor`, `DuckLakeGroupExtractor`,
  `DuckLakeFileExtractor`
- Saude: `SaudeDatasetExtractor`, `SaudeGroupExtractor`,
  `SaudeFileExtractor`

The base classes in :mod:`pysus.api.models` only know that *some*
extractors are registered on an instance; they never hard-code which
one.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from .models import MetadataBag


class MetadataExtractor(ABC):
    """Stateless transformer: a concrete entity → :class:`MetadataBag`.

    Extractor implementations are specific to one client and one
    entity type (dataset / group / file). They read whatever they
    need from the passed object and return a bag carrying only the
    facets they can populate — the ``provenance.origin`` field must
    always be set so :func:`~pysus.api.metadata.models.merge_bags`
    can apply the precedence rules.
    """

    #: The origin label (``"saude"``, ``"ftp"``, ``"dadosgov"``,
    #: ``"ducklake"``). Subclasses override it.
    origin: str = ""

    def extract(self, obj: Any) -> MetadataBag:
        """Synchronous fast path — uses only data already on ``obj``.

        Subclasses implement :meth:`extract`; the default
        :meth:`aextract` just delegates to it. Extractors that need
        network or local-file IO override :meth:`aextract` instead.
        """
        bag = self._extract(obj)
        if not bag.provenance.origin:
            bag.provenance.origin = self.origin
        return bag

    async def aextract(self, obj: Any) -> MetadataBag:
        """Async path — may hit the network or read local files.

        The default implementation delegates to :meth:`extract`.
        """
        return self.extract(obj)

    @abstractmethod
    def _extract(self, obj: Any) -> MetadataBag:
        """Build the bag from ``obj``. Must set ``provenance.origin``."""

    def supported_facets(self) -> set[str]:
        """Return the subset of facet names this extractor populates."""
        return set()


__all__ = ["MetadataExtractor"]
