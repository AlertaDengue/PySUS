from __future__ import annotations

import pathlib
from datetime import datetime as dt
from typing import Annotated, Any, Callable, Dict, List, Optional, Union

from pydantic import BeforeValidator, ConfigDict, Field
from pysus.api.models import BaseRemoteClient  # noqa
from pysus.api.models import BaseRemoteDataset, BaseRemoteFile, BaseRemoteGroup


def to_datetime(value: Any) -> Optional[dt]:
    if not value or not isinstance(value, str) or "Indisponível" in value:
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            return dt.strptime(value, fmt)
        except ValueError:
            continue
    return None


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("sim", "true", "1")


DateTime = Annotated[Optional[dt], BeforeValidator(to_datetime)]


class Resource(BaseRemoteFile):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    title: str = Field(alias="titulo")
    url: str = Field(alias="link")
    api_size: Optional[int] = Field(0, alias="tamanho")
    last_modified: DateTime = Field(None, alias="dataUltimaAtualizacaoArquivo")
    file_name: Optional[str] = Field(None, alias="nomeArquivo")
    type: str = "remote"
    parent: Any = Field(None, exclude=True)

    @property
    def path(self) -> str:
        return self.url

    @property
    def extension(self) -> str:
        if self.file_name:
            return pathlib.Path(self.file_name).suffix
        return pathlib.Path(self.url.split("?")[0]).suffix

    @property
    def size(self) -> int:
        return self.api_size or 0

    @property
    def modify(self) -> dt:
        return self.last_modified or dt.now()

    async def _download(
        self,
        output: Optional[pathlib.Path] = None,
        callback: Optional[Callable[[int], None]] = None,
    ) -> pathlib.Path:
        return await self.client._download_file(
            self, output, callback=callback
        )


class ResourceGroup(BaseRemoteGroup):
    name: str
    group_id: str = Field(exclude=True)

    def __init__(self, name: str, group_id: str, dataset: Dataset):
        super().__init__(dataset=dataset, name=name, group_id=group_id)

    @property
    def long_name(self) -> str:
        return self.name

    @property
    def description(self) -> str:
        return f"Grupo de recursos: {self.name}"

    async def _fetch_files(self) -> List[BaseRemoteFile]:
        return [r for r in self.dataset.resources if r.parent == self]


class Dataset(BaseRemoteDataset):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    title: str = Field(alias="titulo")
    slug: str = Field(alias="nome")
    resources: List[Resource] = Field(default_factory=list, alias="recursos")
    file_updated: DateTime = Field(None, alias="dataUltimaAtualizacaoArquivo")
    group_definitions: Dict[str, str] = Field(
        default_factory=dict, exclude=True
    )

    def model_post_init(self, __context: Any) -> None:
        for resource in self.resources:
            resource.parent = self

    @property
    def name(self) -> str:
        return self.slug

    @property
    def long_name(self) -> str:
        return self.title

    @property
    def description(self) -> str:
        return f"DadosGov Dataset: {self.title}"

    async def _fetch_content(
        self,
    ) -> List[Union[BaseRemoteGroup, BaseRemoteFile]]:
        if not self.resources:
            full_dataset = await self.client.get_dataset(self.id)
            self.resources = full_dataset.resources
            self.model_post_init(None)

        items: List[Union[BaseRemoteGroup, BaseRemoteFile]] = []
        if self.group_definitions:
            for name, group_id in self.group_definitions.items():
                items.append(
                    ResourceGroup(name=name, group_id=group_id, dataset=self)
                )
        items.extend(self.resources)
        return items


Resource.model_rebuild()
Dataset.model_rebuild()
