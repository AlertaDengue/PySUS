"""HTTP client and data models for the dados.gov.br API."""

from __future__ import annotations

import pathlib
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any, Optional

import httpx
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, PrivateAttr
from pysus import __version__
from pysus.api.errors import AuthenticationError, ConnectionError
from pysus.api.models import BaseRemoteClient, BaseRemoteFile
from pysus.api.types import DADOSGOV

if TYPE_CHECKING:  # pragma: no cover
    from .models import Dataset


def to_datetime(value: Any) -> datetime | None:
    """Parse a Brazilian date string into a datetime object.

    Parameters
    ----------
    value : Any
        The value to parse, expected to be a date string in Brazilian format
        (e.g., ``%d/%m/%Y %H:%M:%S`` or ``%d/%m/%Y``).

    Returns
    -------
    datetime or None
        Parsed datetime object, or None if the value cannot be parsed.
    """
    if not value or not isinstance(value, str) or "Indisponível" in value:
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def to_bool(value: Any) -> bool:
    """Parse a Brazilian Portuguese boolean value into a bool.

    Parameters
    ----------
    value : Any
        The value to parse (e.g., ``"sim"``, ``"não"``, ``True``, ``False``).

    Returns
    -------
    bool
        True if the value represents an affirmative, False otherwise.
    """
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
        """Initialize the DadosGov client.

        Parameters
        ----------
        ``**data``
            Additional keyword arguments forwarded to the parent constructor.
        """
        super().__init__(**data)

    @property
    def name(self) -> str:
        """Return the short client name.

        Returns
        -------
        str
            The abbreviated client name ``"DadosGov"``.
        """
        return DADOSGOV

    @property
    def long_name(self) -> str:
        """Return the human-readable client name.

        Returns
        -------
        str
            The full Portuguese name of the portal.
        """
        return "Portal Brasileiro de Dados Abertos"

    @property
    def description(self) -> str:
        """Return a description of the client.

        Returns
        -------
        str
            A Portuguese description of the API interface.
        """
        return "Interface de acesso ao API do Portal de Dados Abertos"

    async def connect(self, token: str | None = None) -> None:
        """Connect to the dados.gov.br API with the given token.

        Parameters
        ----------
        token : str, optional
            The API authentication token. If not provided, uses the
            previously stored token.

        Raises
        ------
        ValueError
            If no token is provided and none was previously stored.
        """
        _token = token or self._token

        if not _token:
            raise AuthenticationError(
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
        """Authenticate with the API.

        Delegates to the :meth:`connect` method.

        Parameters
        ----------
        token : str, optional
            The API authentication token.
        ``**kwargs``
            Additional keyword arguments (currently unused).
        """
        await self.connect(token=token)

    async def close(self) -> None:
        """Close the underlying HTTP client and release resources."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def datasets(self, **kwargs) -> list[Dataset]:
        """Return a list of pre-configured health datasets.

        Returns
        -------
        list[:class:`~pysus.api.dadosgov.models.Dataset`]
            A list of available :class:`~pysus.api.dadosgov.models.Dataset`
            instances for known health databases.
        """
        from .databases import AVAILABLE_DATABASES

        return [db_class(client=self) for db_class in AVAILABLE_DATABASES]

    async def list_datasets(self, **kwargs) -> list[ConjuntoDados]:
        """Search and list available datasets from the portal.

        Parameters
        ----------
        ``**kwargs``
            Search parameters. Supported keys:

            - ``pagina`` (int): Page number for pagination.
            - ``nome_conjunto`` (str): Filter by dataset name.
            - ``dados_abertos`` (bool): Filter by open data flag.
            - ``is_privado`` (bool): Filter by private datasets.
            - ``id_organizacao`` (str): Filter by organisation ID.

        Returns
        -------
        list[ConjuntoDados]
            A list of datasets matching the search criteria.

        Raises
        ------
        ConnectionError
            If the client is not connected.
        """
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
        """Fetch a single dataset by its ID.

        Parameters
        ----------
        id : str
            The unique identifier of the dataset.

        Returns
        -------
        ConjuntoDados
            The requested dataset.

        Raises
        ------
        ConnectionError
            If the client is not connected.
        """
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

    async def download(
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
        """Retrieve the file size from the remote server.

        Makes a HEAD request (falling back to GET with a Range header)
        to determine the Content-Length of the resource.

        Returns
        -------
        int
            The file size in bytes, or 0 if the size could not be determined.
        """
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
