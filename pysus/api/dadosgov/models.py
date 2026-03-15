import requests
from pathlib import Path
from datetime import datetime as dt
from typing import Optional, List, Any, Annotated, Union
from pydantic import BaseModel, Field, BeforeValidator

from pysus import CACHEPATH


def to_datetime(value: Any) -> Optional[dt]:
    if not value or not isinstance(value, str) or "Indisponível" in value:
        return None
    try:
        return dt.strptime(value, "%d/%m/%Y %H:%M:%S")
    except ValueError:
        try:
            return dt.strptime(value, "%d/%m/%Y")
        except ValueError:
            return None


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("sim", "true", "1")


DateTime = Annotated[Optional[dt], BeforeValidator(to_datetime)]
Bool = Annotated[bool, BeforeValidator(to_bool)]


class Tag(BaseModel):
    id: str
    name: str
    display_name: Optional[str] = None

    def __str__(self):
        return self.name


class Resource(BaseModel):
    id: str
    title: str = Field(alias="titulo")
    description: str = Field(alias="descricao")
    url: str = Field(alias="link")
    format: str = Field(alias="formato")
    size: int = Field(alias="tamanho")
    cataloging_date: Optional[str] = Field(None, alias="dataCatalogacao")
    last_modified: Optional[str] = Field(
        None,
        alias="dataUltimaAtualizacaoArquivo",
    )
    download_count: Optional[int] = Field(None, alias="quantidadeDownloads")
    file_name: Optional[str] = Field(None, alias="nomeArquivo")
    resource_type: Optional[str] = Field(None, alias="tipo")
    order_number: Optional[int] = Field(None, alias="numOrdem")
    dataset_id: Optional[str] = Field(None, alias="idConjuntoDados")

    def __str__(self):
        return self.file_name

    def download(self, target_dir: Union[str, Path] = CACHEPATH) -> Path:
        target_path = Path(target_dir)
        target_path.mkdir(parents=True, exist_ok=True)

        output_file = target_path / (
            self.file_name or f"{self.id}.{self.format.lower()}"
        )

        response = requests.get(self.url, stream=True)
        response.raise_for_status()

        with open(output_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return output_file


class DatasetDetail(BaseModel):
    id: str
    title: str = Field(alias="titulo")
    slug: str = Field(alias="nome")
    organization: str = Field(alias="organizacao")
    description: str = Field(alias="descricao")
    license: Optional[str] = Field(None, alias="licenca")
    maintainer: Optional[str] = Field(None, alias="responsavel")
    maintainer_email: Optional[str] = Field(None, alias="emailResponsavel")
    frequency: Optional[str] = Field(None, alias="periodicidade")
    themes: List[Any] = Field(default_factory=list, alias="temas")
    tags: List[Tag] = Field(default_factory=list)
    resources: List[Resource] = Field(default_factory=list, alias="recursos")
    is_open_data: Bool = Field(alias="dadosAbertos")
    is_discontinued: Bool = Field(alias="descontinuado")
    is_private: Bool = Field(False, alias="privado")
    metadata_updated: DateTime = Field(
        None, alias="dataUltimaAtualizacaoMetadados")
    file_updated: DateTime = Field(None, alias="dataUltimaAtualizacaoArquivo")
    cataloging_date: DateTime = Field(None, alias="dataCatalogacao")
    visibility: str = Field(alias="visibilidade")
    status: Optional[str] = Field(None, alias="atualizado")
    seal: Optional[str] = Field(None, alias="selo")
    source: Optional[str] = Field(None, alias="origemCadastro")

    def __str__(self):
        return self.id


class DatasetSummary(BaseModel):
    id: str
    title: str
    name: str = Field(alias="nome")
    organization_name: str = Field(alias="nomeOrganizacao")
    is_updated: Bool = Field(alias="isAtualizado")
    cataloging_date: DateTime = Field(None, alias="catalogacao")
    metadata_modified: DateTime = Field(None, alias="ultimaAlteracaoMetadados")
    last_update: DateTime = Field(None, alias="ultimaAtualizacaoDados")

    def __str__(self):
        return self.name
