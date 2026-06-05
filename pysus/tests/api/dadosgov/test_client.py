"""Tests for pysus.api.dadosgov.client."""

from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from pysus import __version__
from pysus.api.dadosgov.client import (
    ConjuntoDados,
    DadosGov,
    Recurso,
    to_bool,
    to_datetime,
)


class TestToDatetime:
    def test_valid_datetime_string(self):
        result = to_datetime("01/02/2024 10:30:00")
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 2
        assert result.day == 1
        assert result.hour == 10
        assert result.minute == 30
        assert result.second == 0

    def test_valid_date_string(self):
        result = to_datetime("15/03/2024")
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 3
        assert result.day == 15

    def test_none_value(self):
        assert to_datetime(None) is None

    def test_empty_string(self):
        assert to_datetime("") is None

    def test_indisponivel_value(self):
        assert to_datetime("Indisponível") is None

    def test_indisponivel_with_accent(self):
        assert to_datetime("Dado Indisponível") is None

    def test_invalid_string(self):
        assert to_datetime("not-a-date") is None

    def test_non_string_non_none(self):
        assert to_datetime(12345) is None


class TestToBool:
    def test_true_bool(self):
        assert to_bool(True) is True

    def test_false_bool(self):
        assert to_bool(False) is False

    def test_sim_string(self):
        assert to_bool("sim") is True

    def test_nao_string(self):
        assert to_bool("não") is False

    def test_true_string(self):
        assert to_bool("true") is True

    def test_false_string(self):
        assert to_bool("false") is False

    def test_1_string(self):
        assert to_bool("1") is True

    def test_0_string(self):
        assert to_bool("0") is False

    def test_Sim_capitalized(self):
        assert to_bool("Sim") is True

    def test_TRUE_uppercase(self):
        assert to_bool("TRUE") is True

    def test_arbitrary_string(self):
        assert to_bool("qualquer") is False

    def test_integer_one(self):
        assert to_bool(1) is True

    def test_integer_zero(self):
        assert to_bool(0) is False


class TestRecurso:
    def test_fields_from_aliases(self):
        r = Recurso(
            id="r1",
            titulo="Arquivo CSV",
            link="https://example.com/file.csv",
            tamanho=1024,
            dataUltimaAtualizacaoArquivo="10/05/2024",
            nomeArquivo="dados.csv",
        )
        assert r.id == "r1"
        assert r.title == "Arquivo CSV"
        assert r.url == "https://example.com/file.csv"
        assert r.api_size == 1024
        assert isinstance(r.last_modified, datetime)
        assert r.file_name == "dados.csv"

    def test_fields_from_names(self):
        r = Recurso(
            id="r2",
            title="CSV File",
            url="https://example.com/data.zip",
            api_size=2048,
            file_name="data.zip",
        )
        assert r.id == "r2"
        assert r.title == "CSV File"
        assert r.url == "https://example.com/data.zip"
        assert r.api_size == 2048
        assert r.file_name == "data.zip"
        assert r.last_modified is None

    def test_last_modified_none_when_indisponivel(self):
        r = Recurso(
            id="r3",
            title="No Date",
            url="https://example.com/file",
            api_size=0,
            dataUltimaAtualizacaoArquivo="Indisponível",
        )
        assert r.last_modified is None

    @pytest.mark.asyncio
    async def test_get_size_head_success(self):
        r = Recurso(
            id="r4",
            title="Test",
            url="https://example.com/file.csv",
            api_size=0,
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Length": "5000"}

        mock_client = AsyncMock()
        mock_client.head.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client

        with patch("httpx.AsyncClient", return_value=mock_client):
            size = await r.get_size()

        assert size == 5000
        mock_client.head.assert_called_once_with("https://example.com/file.csv")

    @pytest.mark.asyncio
    async def test_get_size_head_405_fallback_to_get(self):
        r = Recurso(
            id="r5",
            title="Test",
            url="https://example.com/file.csv",
            api_size=0,
        )
        head_response = MagicMock()
        head_response.status_code = 405

        get_response = MagicMock()
        get_response.headers = {"Content-Length": "3000"}

        mock_client = AsyncMock()
        mock_client.head.return_value = head_response
        mock_client.get.return_value = get_response
        mock_client.__aenter__.return_value = mock_client

        with patch("httpx.AsyncClient", return_value=mock_client):
            size = await r.get_size()

        assert size == 3000
        mock_client.head.assert_called_once()
        mock_client.get.assert_called_once_with(
            "https://example.com/file.csv", headers={"Range": "bytes=0-0"}
        )

    @pytest.mark.asyncio
    async def test_get_size_no_content_length(self):
        r = Recurso(
            id="r6",
            title="Test",
            url="https://example.com/file.csv",
            api_size=0,
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}

        mock_client = AsyncMock()
        mock_client.head.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client

        with patch("httpx.AsyncClient", return_value=mock_client):
            size = await r.get_size()

        assert size == 0


class TestConjuntoDados:
    def test_fields_from_aliases(self):
        c = ConjuntoDados(
            id="c1",
            titulo="Dataset Teste",
            nome="dataset-teste",
            recursos=[
                Recurso(
                    id="r1",
                    titulo="Resource",
                    link="http://example.com",
                    tamanho=100,
                )
            ],
        )
        assert c.id == "c1"
        assert c.title == "Dataset Teste"
        assert c.slug == "dataset-teste"
        assert len(c.resources) == 1
        assert c.resources[0].id == "r1"

    def test_fields_from_names(self):
        c = ConjuntoDados(id="c2", title="Dataset", slug="dataset")
        assert c.id == "c2"
        assert c.title == "Dataset"
        assert c.slug == "dataset"
        assert c.resources == []


class TestDadosGov:
    def test_name(self):
        client = DadosGov()
        assert client.name == "DadosGov"

    def test_long_name(self):
        client = DadosGov()
        assert client.long_name == "Portal Brasileiro de Dados Abertos"

    def test_description(self):
        client = DadosGov()
        expected = "Interface de acesso ao API do Portal de Dados Abertos"
        assert client.description == expected

    @pytest.mark.asyncio
    async def test_connect_with_token_creates_client(self):
        client = DadosGov()
        assert client._client is None

        with patch("httpx.AsyncClient") as mock_async_client:
            await client.connect(token="test-token-123")

        assert client._token == "test-token-123"
        assert client._client is not None
        mock_async_client.assert_called_once_with(
            base_url="https://dados.gov.br/dados/api",
            headers={
                "Accept": "application/json",
                "User-Agent": f"PySUS/{__version__}",
                "chave-api-dados-abertos": "test-token-123",
            },
            timeout=30.0,
            follow_redirects=True,
        )

    @pytest.mark.asyncio
    async def test_connect_without_token_raises_value_error(self):
        client = DadosGov()
        with pytest.raises(
            ValueError, match="A token is required to connect to DadosGov"
        ):
            await client.connect(token=None)

    @pytest.mark.asyncio
    async def test_connect_with_existing_token(self):
        client = DadosGov()
        client._token = "existing-token"

        with patch("httpx.AsyncClient") as mock_async_client:
            await client.connect()

        assert client._token == "existing-token"
        mock_async_client.assert_called_once()

    @pytest.mark.asyncio
    async def test_connect_replaces_existing_client(self):
        client = DadosGov()
        old_close = AsyncMock()
        old_client = AsyncMock()
        old_client.aclose = old_close
        client._client = old_client

        with patch("httpx.AsyncClient"):
            await client.connect(token="new-token")

        old_close.assert_awaited_once()
        assert client._token == "new-token"

    @pytest.mark.asyncio
    async def test_login_delegates_to_connect(self):
        client = DadosGov()
        with patch(
            "pysus.api.dadosgov.client.DadosGov.connect"
        ) as mock_connect:
            mock_connect.return_value = None
            await client.login(token="login-token")
        mock_connect.assert_awaited_once_with(token="login-token")

    @pytest.mark.asyncio
    async def test_login_with_kwargs(self):
        client = DadosGov()
        with patch(
            "pysus.api.dadosgov.client.DadosGov.connect"
        ) as mock_connect:
            mock_connect.return_value = None
            await client.login(token="t", extra_param="ignored")
        mock_connect.assert_awaited_once_with(token="t")

    @pytest.mark.asyncio
    async def test_close_with_active_client(self):
        client = DadosGov()
        mock_http = AsyncMock()
        client._client = mock_http

        await client.close()

        mock_http.aclose.assert_awaited_once()
        assert client._client is None

    @pytest.mark.asyncio
    async def test_close_without_client(self):
        client = DadosGov()
        client._client = None
        await client.close()
        assert client._client is None

    @pytest.mark.asyncio
    async def test_datasets_returns_list(self):
        client = DadosGov()
        result = await client.datasets()
        assert isinstance(result, list)
        assert len(result) > 0
        from pysus.api.dadosgov.databases import AVAILABLE_DATABASES

        assert len(result) == len(AVAILABLE_DATABASES)
        for ds in result:
            assert ds.client is client

    @pytest.mark.asyncio
    async def test_list_datasets_connection_error(self):
        client = DadosGov()
        client._client = None
        with pytest.raises(ConnectionError, match="Client not connected"):
            await client.list_datasets()

    @pytest.mark.asyncio
    async def test_list_datasets_success(self):
        client = DadosGov()
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "id": "ds1",
                "titulo": "Dataset 1",
                "nome": "ds-1",
                "recursos": [],
            },
            {
                "id": "ds2",
                "titulo": "Dataset 2",
                "nome": "ds-2",
                "recursos": [
                    {
                        "id": "r1",
                        "titulo": "Resource",
                        "link": "http://example.com",
                        "tamanho": 50,
                    }
                ],
            },
        ]
        mock_http.get.return_value = mock_response
        client._client = mock_http

        results = await client.list_datasets(
            pagina=2,
            nome_conjunto="teste",
            dados_abertos=True,
            id_organizacao="org1",
        )

        assert len(results) == 2
        assert results[0].id == "ds1"
        assert results[1].id == "ds2"
        assert len(results[1].resources) == 1
        mock_http.get.assert_awaited_once_with(
            "publico/conjuntos-dados",
            params={
                "pagina": 2,
                "nomeConjuntoDados": "teste",
                "dadosAbertos": True,
                "isPrivado": False,
                "idOrganizacao": "org1",
            },
        )

    @pytest.mark.asyncio
    async def test_list_datasets_minimal_params(self):
        client = DadosGov()
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_http.get.return_value = mock_response
        client._client = mock_http

        results = await client.list_datasets()

        assert results == []
        mock_http.get.assert_awaited_once_with(
            "publico/conjuntos-dados",
            params={"pagina": 1, "isPrivado": False},
        )

    @pytest.mark.asyncio
    async def test_get_dataset_connection_error(self):
        client = DadosGov()
        client._client = None
        with pytest.raises(ConnectionError, match="Client not connected"):
            await client.get_dataset("some-id")

    @pytest.mark.asyncio
    async def test_get_dataset_success(self):
        client = DadosGov()
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "id": "ds1",
            "titulo": "Single Dataset",
            "nome": "single-ds",
            "recursos": [],
        }
        mock_http.get.return_value = mock_response
        client._client = mock_http

        result = await client.get_dataset("ds1")

        assert result.id == "ds1"
        assert result.title == "Single Dataset"
        mock_http.get.assert_awaited_once_with("publico/conjuntos-dados/ds1")

    @pytest.mark.asyncio
    async def test_download_file_connection_error(self):
        client = DadosGov()
        client._client = None
        mock_file = MagicMock()
        mock_file.path = "http://example.com/file.csv"
        with pytest.raises(ConnectionError, match="Client not connected"):
            await client._download_file(mock_file, Path("/tmp/out.csv"))

    @pytest.mark.asyncio
    async def test_download_file_success(self, tmp_path):
        client = DadosGov()
        mock_http = AsyncMock(spec=httpx.AsyncClient)

        async def _aiter_bytes():
            yield b"12345"
            yield b"67890"

        mock_response = MagicMock()
        mock_response.headers = {"Content-Length": "10"}
        mock_response.aiter_bytes = _aiter_bytes

        cm = AsyncMock()
        cm.__aenter__.return_value = mock_response
        cm.__aexit__.return_value = None

        mock_http.stream.return_value = cm
        client._client = mock_http

        mock_file = MagicMock()
        mock_file.path = "https:/example.com/file.csv"

        output = tmp_path / "test_download.csv"
        callback = MagicMock()

        try:
            result = await client._download_file(
                mock_file, output, callback=callback
            )

            assert result == output
            mock_http.stream.assert_called_once_with(
                "GET", "https://example.com/file.csv"
            )
            assert output.read_bytes() == b"1234567890"
            assert callback.call_count == 2
            callback.assert_any_call(5, 10)
            callback.assert_any_call(10, 10)
        finally:
            if output.exists():
                output.unlink()

    @pytest.mark.asyncio
    async def test_download_file_no_callback(self, tmp_path):
        client = DadosGov()
        mock_http = AsyncMock(spec=httpx.AsyncClient)

        async def _aiter_bytes():
            yield b"data"

        mock_response = MagicMock()
        mock_response.headers = {"Content-Length": "4"}
        mock_response.aiter_bytes = _aiter_bytes

        cm = AsyncMock()
        cm.__aenter__.return_value = mock_response
        cm.__aexit__.return_value = None

        mock_http.stream.return_value = cm
        client._client = mock_http

        mock_file = MagicMock()
        mock_file.path = "http:/example.com/file.csv"

        output = tmp_path / "test_download_nocb.csv"

        try:
            result = await client._download_file(mock_file, output)

            assert result == output
            mock_http.stream.assert_called_once_with(
                "GET", "http://example.com/file.csv"
            )
        finally:
            if output.exists():
                output.unlink()
