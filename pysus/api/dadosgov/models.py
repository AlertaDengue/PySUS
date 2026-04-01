import zipfile
import requests
import urllib3
from pathlib import Path
from datetime import datetime as dt
from typing import Optional, List, Any, Annotated, Union
from pydantic import BaseModel, Field, BeforeValidator, field_validator

from pysus import CACHEPATH
from pysus.api.models import FileDescription

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
    description: Optional[str] = Field(None, alias="descricao")
    url: str = Field(alias="link")
    format: str = Field(alias="formato")
    api_size: int = Field(alias="tamanho")
    cataloging_date: Optional[str] = Field(None, alias="dataCatalogacao")
    last_modified: Optional[str | dt] = Field(
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

    @field_validator("last_modified", mode="before")
    @classmethod
    def parse_date(cls, v: Optional[str]) -> Optional[dt]:
        if not v or isinstance(v, dt):
            return v
        try:
            return dt.strptime(v, "%d/%m/%Y")
        except ValueError:
            return None

    @property
    def basename(self) -> str:
        name = self.url.split("/")[-1]
        return name.rstrip(".zip").replace("_csv", ".csv")

    @property
    def size(self) -> int:
        try:
            response = requests.head(
                self.url,
                verify=False,
                allow_redirects=True,
                timeout=5,
            )
            return int(response.headers.get("Content-Length", 0))
        except (requests.RequestException, ValueError):
            return self.api_size

    def download(self, target_dir: Union[str, Path] = CACHEPATH) -> Path:
        target_path = Path(target_dir)
        target_path.mkdir(parents=True, exist_ok=True)

        tmp_file = target_path / f"{self.id}.download"

        response = requests.get(self.url, stream=True, verify=False)
        response.raise_for_status()

        with open(tmp_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        if zipfile.is_zipfile(tmp_file):
            with zipfile.ZipFile(tmp_file) as z:
                members = z.namelist()

                if len(members) == 1:
                    name = members[0]
                    output_file = target_path / name
                    z.extract(name, target_path)
                else:
                    z.extractall(target_path)
                    output_file = target_path

            tmp_file.unlink()
            return output_file

        output_file = target_path / (
            self.file_name or f"{self.id}.{self.format.lower()}"
        )

        tmp_file.rename(output_file)

        return output_file


class Dataset(BaseModel):
    id: str
    title: str = Field(alias="titulo")
    slug: str = Field(alias="nome")
    organization: str = Field(alias="organizacao")
    description: Optional[str] = Field(None, alias="descricao")
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

    def describe(self, resource: Resource) -> FileDescription:
        return FileDescription(
            name=resource.basename,
            group=self.slug,
            year=int,
            size=resource.size,
            last_update=resource.last_modified or self.file_updated or dt.now(),
            uf=None,
            month=None,
            disease=self.title,
        )


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
