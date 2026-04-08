from __future__ import annotations

import pathlib
from typing import Callable, Dict, List, Optional

import httpx
from pydantic import PrivateAttr
from pysus import __version__
from pysus.api.models import BaseRemoteClient, BaseRemoteDataset, BaseRemoteFile

from .models import Dataset


class DadosGov(BaseRemoteClient):
    base_url: str = "https://dados.gov.br/dados/api"

    _token: Optional[str] = PrivateAttr(default=None)
    _client: Optional[httpx.AsyncClient] = PrivateAttr(default=None)

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

    async def connect(self, token: Optional[str] = None) -> None:
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

    async def login(self, token: Optional[str] = None, **kwargs) -> None:
        await self.connect(token=token)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def datasets(self, **kwargs) -> List[Dataset]:
        from .databases import AVAILABLE_DATABASES

        return [db_class(client=self) for db_class in AVAILABLE_DATABASES]

    async def list_datasets(self, **kwargs) -> List[Dataset]:
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
        return [Dataset(**item, client=self) for item in data]

    async def get_dataset(
        self, id: str, group_definitions: Optional[Dict[str, str]] = None
    ) -> Dataset:
        if self._client is None:
            raise ConnectionError(
                "Client not connected. Call login(token=...) first.",
            )

        response = await self._client.get(f"publico/conjuntos-dados/{id}")
        response.raise_for_status()

        return Dataset(
            **response.json(),
            client=self,
            group_definitions=group_definitions or {},
        )

    async def _download_file(
        self,
        file: BaseRemoteFile,
        output: pathlib.Path,
        callback: Optional[Callable[[int], None]] = None,
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
