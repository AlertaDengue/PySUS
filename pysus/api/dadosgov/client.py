from __future__ import annotations

import pathlib
from collections.abc import Callable
from datetime import datetime
from typing import Annotated, Any, Dict, List, Optional

import httpx
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, PrivateAttr
from pysus import __version__
from pysus.api.models import BaseRemoteClient, BaseRemoteFile


def to_datetime(value: Any) -> datetime | None:
    if not value or not isinstance(value, str) or "Indisponível" in value:
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("sim", "true", "1")


DateTime = Annotated[Optional[datetime], BeforeValidator(to_datetime)]
Bool = Annotated[bool, BeforeValidator(to_bool)]


class DadosGov(BaseRemoteClient):
    base_url: str = "https://dados.gov.br/dados/api"

    _token: str | None = PrivateAttr(default=None)
    _client: httpx.AsyncClient | None = PrivateAttr(default=None)

    def __init__(self, **data):
        super().__init__(**data)

    @property
    def name(self) -> str:
        return "DadosGov"

    @property
    def long_name(self) -> str:
        return "Portal Brasileiro de Dados Abertos"

    @property
    def description(self) -> str:
        return "Interface de acesso ao API do Portal de Dados Abertos"

    async def connect(self, token: str | None = None) -> None:
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
            timeout=60.0,
            follow_redirects=True,
        )

    async def login(self, token: str | None = None, **kwargs) -> None:
        await self.connect(token=token)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def datasets(self, **kwargs) -> list[ConjuntoDados]:
        from .databases import AVAILABLE_DATABASES

        return [db_class(client=self) for db_class in AVAILABLE_DATABASES]

    async def list_datasets(self, **kwargs) -> list[ConjuntoDados]:
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

    async def get_dataset(
        self, id: str, group_definitions: dict[str, str] | None = None
    ) -> ConjuntoDados:
        if self._client is None:
            raise ConnectionError(
                "Client not connected. Call login(token=...) first.",
            )

        response = await self._client.get(f"publico/conjuntos-dados/{id}")
        response.raise_for_status()

        return ConjuntoDados(
            **response.json(),
            client=self,
            group_definitions=group_definitions or {},
        )

    async def _download_file(
        self,
        file: BaseRemoteFile,
        output: pathlib.Path,
        callback: Callable[[int], None] | None = None,
    ) -> pathlib.Path:
        if self._client is None:
            raise ConnectionError(
                "Client not connected. Call login(token=...) first.",
            )

        async with self._client.stream("GET", str(file.path)) as response:
            response.raise_for_status()
            with open(output, "wb") as f:
                async for chunk in response.aiter_bytes():
                    f.write(chunk)
                    if callback:
                        callback(len(chunk))
        return output


class Recurso(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    title: str = Field(alias="titulo")
    url: str = Field(alias="link")
    api_size: int = Field(alias="tamanho")
    last_modified: DateTime = Field(None, alias="dataUltimaAtualizacaoArquivo")
    file_name: str | None = Field(None, alias="nomeArquivo")

    async def get_size(self) -> int:
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
    model_config = ConfigDict(populate_by_name=True)

    id: str
    title: str = Field(alias="titulo")
    slug: str = Field(alias="nome")
    resources: list[Recurso] = Field(default_factory=list, alias="recursos")
