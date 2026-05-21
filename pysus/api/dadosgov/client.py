"""HTTP client and data models for the dados.gov.br API."""

from __future__ import annotations

import pathlib
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any, Optional

import httpx
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, PrivateAttr
from pysus import __version__
from pysus.api.models import BaseRemoteClient, BaseRemoteFile

if TYPE_CHECKING:
    from .models import Dataset


def to_datetime(value: Any) -> datetime | None:
    """Parse a Brazilian date string into a datetime object."""
    if not value or not isinstance(value, str) or "Indisponível" in value:
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def to_bool(value: Any) -> bool:
    """Parse a Brazilian Portuguese boolean value ("sim"/"não") into a bool."""
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("sim", "true", "1")


DateTime = Annotated[Optional[datetime], BeforeValidator(to_datetime)]
Bool = Annotated[bool, BeforeValidator(to_bool)]


class DadosGov(BaseRemoteClient):
    """Client for the dados.gov.br open data portal API."""

    base_url: str = "https://dados.gov.br/dados/api"

    _token: str | None = PrivateAttr(default=None)
    _client: httpx.AsyncClient | None = PrivateAttr(default=None)

    def __init__(self, **data):
        """Initialize the DadosGov client."""
        super().__init__(**data)

    @property
    def name(self) -> str:
        """Return the short client name."""
        return "DadosGov"

    @property
    def long_name(self) -> str:
        """Return the human-readable client name."""
        return "Portal Brasileiro de Dados Abertos"

    @property
    def description(self) -> str:
        """Return a description of the client."""
        return "Interface de acesso ao API do Portal de Dados Abertos"

    async def connect(self, token: str | None = None) -> None:
        """Connect to the dados.gov.br API with the given token."""
        _token = token or self._token

        if not _token:
            raise ValueError(
                "A token is required to connect to DadosGov. "
                "Pass it to connect(token=...) or login(token=...)."
            )

        self._token = _token

        if self._client:
            await self.close()

        headers = {
            "Accept": "application/json",
            "User-Agent": f"PySUS/{__version__}",
            "chave-api-dados-abertos": self._token,
        }

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=30.0,
            follow_redirects=True,
        )

    async def login(self, token: str | None = None, **kwargs) -> None:
        """Authenticate with the API (delegates to connect)."""
        await self.connect(token=token)

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def datasets(self, **kwargs) -> list[Dataset]:
        """Return a list of pre-configured health datasets."""
        from .databases import AVAILABLE_DATABASES

        return [db_class(client=self) for db_class in AVAILABLE_DATABASES]

    async def list_datasets(self, **kwargs) -> list[ConjuntoDados]:
        """Search and list available datasets from the portal."""
        if self._client is None:
            raise ConnectionError(
                "Client not connected. Call login(token=...) first.",
            )

        params = {
            "pagina": kwargs.get("pagina", 1),
            "nomeConjuntoDados": kwargs.get("nome_conjunto"),
            "dadosAbertos": kwargs.get("dados_abertos"),
            "isPrivado": kwargs.get("is_privado", False),
            "idOrganizacao": kwargs.get("id_organizacao"),
        }
        params = {k: v for k, v in params.items() if v is not None}

        response = await self._client.get(
            "publico/conjuntos-dados",
            params=params,
        )
        response.raise_for_status()

        data = response.json()
        return [ConjuntoDados(**item, client=self) for item in data]

    async def get_dataset(self, id: str) -> ConjuntoDados:
        """Fetch a single dataset by its ID."""
        if self._client is None:
            raise ConnectionError(
                "Client not connected. Call login(token=...) first.",
            )

        response = await self._client.get(f"publico/conjuntos-dados/{id}")
        response.raise_for_status()

        return ConjuntoDados(
            **response.json(),
            client=self,
        )

    async def _download_file(
        self,
        file: BaseRemoteFile,
        output: pathlib.Path,
        callback: Callable[[int, int], None] | None = None,
    ) -> pathlib.Path:
        """Download a remote file to a local path."""
        if self._client is None:
            raise ConnectionError(
                "Client not connected. Call login(token=...) first.",
            )

        url = (
            str(file.path)
            .replace("https:/", "https://")
            .replace("http:/", "http://")
        )

        async with self._client.stream("GET", url) as response:
            response.raise_for_status()
            total = int(response.headers.get("Content-Length", 0))
            downloaded = 0
            with open(output, "wb") as f:
                async for chunk in response.aiter_bytes():
                    f.write(chunk)
                    downloaded += len(chunk)
                    if callback:
                        callback(downloaded, total)
        return output


class Recurso(BaseModel):
    """A single resource (file) within a dataset on dados.gov.br."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    title: str = Field(alias="titulo")
    url: str = Field(alias="link")
    api_size: int = Field(alias="tamanho")
    last_modified: DateTime = Field(None, alias="dataUltimaAtualizacaoArquivo")
    file_name: str | None = Field(None, alias="nomeArquivo")

    async def get_size(self) -> int:
        """Retrieve the file size from the remote server."""
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.head(self.url)

            if response.status_code == 405:
                response = await client.get(
                    self.url,
                    headers={"Range": "bytes=0-0"},
                )

            size = response.headers.get("Content-Length")
            return int(size) if size else 0


class ConjuntoDados(BaseModel):
    """A dataset group as returned by the dados.gov.br API."""

    model_config = ConfigDict(populate_by_name=True)
    client: BaseRemoteClient | None = None

    id: str
    title: str = Field(alias="titulo")
    slug: str = Field(alias="nome")
    resources: list[Recurso] = Field(default_factory=list, alias="recursos")
