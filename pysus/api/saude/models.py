"""Remote models for the Saude (dadosabertos.saude.gov.br) client.

Maps the portal's CKAN catalog into the PySUS remote hierarchy:

- :class:`SaudeDataset` — one theme dataset (from
  :mod:`~pysus.api.saude.databases` specs); its content is the set of
  CKAN packages matched by the spec's group/patterns;
- :class:`SaudeGroup` — one CKAN package (e.g.
  ``arboviroses-dengue``); its files are the package resources;
- :class:`SaudeFile` — one downloadable resource (CSV/JSON/XML/PDF).

The same logical dataset also exists on other sources (dados.gov.br,
DATASUS FTP) — those keep their own declarations; linkage across
sources happens at merge time via ``identity.cross_origin_id`` (the
shared CKAN UUID), never by collapsing declarations here.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import ClassVar, cast

from pydantic import Field, PrivateAttr
from pysus.api.models import BaseRemoteDataset, BaseRemoteFile, BaseRemoteGroup
from pysus.api.types import State

from .databases import DatasetSpec, parse_year
from .download import download_resource
from .errors import ResourceNotFound
from .metadata import (
    SaudeDatasetExtractor,
    SaudeFileExtractor,
    SaudeGroupExtractor,
)
from .resources import CatalogEntry, CKANPackage, Resource

__all__ = ["SaudeDataset", "SaudeFile", "SaudeGroup"]


class SaudeFile(BaseRemoteFile):
    """A downloadable resource of a CKAN package on OpenDataSUS."""

    record: Resource
    type: str = "File"
    extractor_types: ClassVar[list] = [SaudeFileExtractor]

    @property
    def extension(self) -> str:
        """Return the resource format lower-cased (e.g. ``.csv.zip``)."""
        fmt = (self.record.format or "").lower()
        return f".{fmt}" if fmt else Path(self.record.url).suffix

    @property
    def size(self) -> int:
        """Return the resource size in bytes (0 when unknown)."""
        return self.record.size or 0

    @property
    def modify(self) -> datetime:
        """Return the last modification timestamp of the resource.

        Raises
        ------
        ValueError
            If the resource carries no modification date.
        """
        m = self.record.last_modified or self.record.metadata_modified
        if not m:
            raise ValueError("File requires a modify date")
        return m

    @property
    def year(self) -> int | None:
        """Return the year parsed from the resource name, if any."""
        return parse_year(self.record.name)

    @property
    def month(self) -> int | None:
        """Return the month — resource names carry no month info."""
        return None

    @property
    def state(self) -> State | None:
        """Return the state — resources are national scope."""
        return None

    async def _download(
        self,
        output: Path | None = None,
        callback: Callable[[int, int], None] | None = None,
    ) -> Path:
        """Download the resource to *output*."""
        if output is None:
            output = Path(f"./{self.basename}")
        group = cast("SaudeGroup", self.group)
        package = await group.fetch_package()
        client = cast("SaudeClient", self.client)
        return await download_resource(
            client._client,
            package,
            resource_id=self.record.id,
            dest_dir=output.parent,
            progress=callback,
        )

    async def fetch_size(self) -> int:
        """Fetch the resource size from the remote server."""
        return self.record.size or 0


class SaudeGroup(BaseRemoteGroup):
    """One CKAN package (dataset) inside a Saude theme dataset."""

    entry: CatalogEntry
    extractor_types: ClassVar[list] = [SaudeGroupExtractor]
    _package: CKANPackage | None = PrivateAttr(default=None)

    @property
    def name(self) -> str:
        """Return the package slug."""
        return self.entry.name

    @property
    def long_name(self) -> str:
        """Return the package title."""
        return self.entry.title

    @property
    def description(self) -> str:
        """Return the package notes."""
        return self.entry.notes or ""

    async def fetch_package(self, use_cache: bool = True) -> CKANPackage:
        """Fetch (and cache) the full CKAN package for this group."""
        if self._package is None:
            client = cast("SaudeClient", self.dataset.client)
            self._package = await client.fetch_dataset(
                self.entry.name, use_cache=use_cache
            )
        return self._package

    @property
    async def package(self) -> CKANPackage:
        """The full CKAN package (fetched lazily)."""
        return await self.fetch_package()

    async def _fetch_files(self) -> list[BaseRemoteFile]:
        """Build SaudeFile objects from the package resources.

        Resources with format ``API`` are documentation links and are
        skipped; PDF dictionaries are kept (they document the columns).
        """
        package = await self.fetch_package()
        files: list[BaseRemoteFile] = []
        for resource in package.resources:
            if (resource.format or "").upper() == "API":
                continue
            files.append(
                SaudeFile(
                    record=resource,
                    dataset=self.dataset,
                    group=self,
                    path=Path(resource.url),
                )
            )
        return files

    async def resource(self, resource_id: str) -> Resource:
        """Return the resource with the given id, if it exists."""
        package = await self.fetch_package()
        for resource in package.resources:
            if resource.id == resource_id:
                return resource
        raise ResourceNotFound(
            f"Resource '{resource_id}' not found in '{self.name}'."
        )


class SaudeDataset(BaseRemoteDataset):
    """A theme dataset of the OpenDataSUS portal.

    Instances are created from a :class:`DatasetSpec` and expose the
    portal's packages (via ``content`` → :class:`SaudeGroup`) and the
    DEMAS endpoints (via ``spec.endpoints`` — Stage 3 turns these
    into queryable files).
    """

    spec: DatasetSpec = Field(exclude=True)
    client: SaudeClient = Field(exclude=True)
    extractor_types: ClassVar[list] = [SaudeDatasetExtractor]

    @property
    def name(self) -> str:
        """Return the canonical dataset name (e.g. ``SISAGUA``)."""
        return self.spec.name

    @property
    def long_name(self) -> str:
        """Return the human-readable dataset name."""
        return self.spec.long_name

    @property
    def description(self) -> str:
        """Return the dataset description."""
        return self.spec.description

    @property
    def endpoints(self) -> tuple[str, ...]:
        """Return the DEMAS REST endpoints for this dataset."""
        return self.spec.endpoints

    async def _fetch_content(self) -> list[SaudeGroup]:
        """Return the SaudeGroups (CKAN packages) of this dataset."""
        spec = self.spec
        groups: list[SaudeGroup] = []
        if spec.ckan_group:
            async for entry in self.client.iter_datasets(group=spec.ckan_group):
                if spec.matches(entry.name):
                    groups.append(SaudeGroup(entry=entry, dataset=self))
        else:
            async for entry in self.client.iter_datasets():
                if spec.matches(entry.name):
                    groups.append(SaudeGroup(entry=entry, dataset=self))
        return groups


# Rebuild pydantic models with postponed annotations so their fields
# are fully defined (the ``client: SaudeClient`` forward reference
# cannot resolve at class-definition time).
import pydantic  # noqa: E402

from .client import SaudeClient  # noqa: E402

for _model in list(globals().values()):
    if (
        isinstance(_model, type)
        and issubclass(_model, pydantic.BaseModel)
        and _model.__module__ == __name__
    ):
        try:
            _model.model_rebuild(_types_namespace={"SaudeClient": SaudeClient})
        except Exception:  # noqa: B902 — rebuild best effort
            pass
