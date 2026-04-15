from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pysus.api.dadosgov.client import (
    ConjuntoDados,
    DadosGov,
    Recurso,
    to_bool,
    to_datetime,
)


class TestToDatetime:
    def test_to_datetime_valid_format(self):
        result = to_datetime("01/01/2026 12:00:00")
        assert result == datetime(2026, 1, 1, 12, 0, 0)

    def test_to_datetime_short_format(self):
        result = to_datetime("01/01/2026")
        assert result == datetime(2026, 1, 1)

    def test_to_datetime_none_for_invalid(self):
        assert to_datetime(None) is None
        assert to_datetime("Indisponível") is None
        assert to_datetime("invalid") is None


class TestToBool:
    def test_to_bool_true_values(self):
        assert to_bool(True) is True
        assert to_bool("sim") is True
        assert to_bool("true") is True
        assert to_bool("1") is True

    def test_to_bool_false_values(self):
        assert to_bool(False) is False
        assert to_bool("false") is False
        assert to_bool("0") is False


class TestDadosGov:
    @pytest.mark.asyncio
    async def test_dadosgov_init(self):
        client = DadosGov()
        assert client.name == "DadosGov"
        assert client.long_name == "Portal Brasileiro de Dados Abertos"
        assert (
            client.description
            == "Interface de acesso ao API do Portal de Dados Abertos"
        )

    @pytest.mark.asyncio
    async def test_connect_requires_token(self):
        client = DadosGov()
        with pytest.raises(ValueError, match="token is required"):
            await client.connect()

    @pytest.mark.asyncio
    async def test_login_sets_token(self):
        client = DadosGov()
        await client.login(token="test_token_123")
        assert client._token == "test_token_123"
        await client.close()

    @pytest.mark.asyncio
    async def test_connect_creates_client(self):
        client = DadosGov()
        await client.connect(token="test_token")
        assert client._client is not None
        await client.close()

    @pytest.mark.asyncio
    async def test_close_clears_client(self):
        client = DadosGov()
        await client.connect(token="test_token")
        await client.close()
        assert client._client is None

    @pytest.mark.asyncio
    async def test_datasets_returns_databases(self):
        client = DadosGov()
        with patch("pysus.api.dadosgov.client.AVAILABLE_DATABASES", []):
            result = await client.datasets()
            assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_list_datasets_requires_connection(self):
        client = DadosGov()
        with pytest.raises(ConnectionError, match="not connected"):
            await client.list_datasets()

    @pytest.mark.asyncio
    async def test_list_datasets_calls_api(self):
        client = DadosGov()
        await client.connect(token="test_token")

        with patch.object(client._client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = [{"id": "1", "titulo": "Test"}]
            mock_get.return_value = mock_response

            result = await client.list_datasets()
            mock_get.assert_called_once()
            assert len(result) == 1

        await client.close()

    @pytest.mark.asyncio
    async def test_get_dataset_calls_api(self):
        client = DadosGov()
        await client.connect(token="test_token")

        with patch.object(client._client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "id": "1",
                "titulo": "Test",
                "nome": "test",
                "recursos": [],
            }
            mock_get.return_value = mock_response

            result = await client.get_dataset("1")
            mock_get.assert_called_once_with("publico/conjuntos-dados/1")
            assert result.id == "1"

        await client.close()


class TestRecurso:
    @pytest.mark.asyncio
    async def test_recurso_get_size(self):
        recurso = Recurso(
            id="1",
            titulo="Test",
            link="http://example.com/file.zip",
            tamanho=1000,
        )

        with patch(
            "pysus.api.dadosgov.client.httpx.AsyncClient"
        ) as mock_client_class:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.headers = {"Content-Length": "5000"}

            mock_instance = AsyncMock()
            mock_instance.head.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = (
                mock_instance
            )

            size = await recurso.get_size()
            assert size == 5000


class TestConjuntoDados:
    def test_conjunto_dados_creation(self):
        data = {
            "id": "test_id",
            "titulo": "Test Dataset",
            "nome": "test-dataset",
            "recursos": [],
        }
        conjunto = ConjuntoDados(**data)
        assert conjunto.id == "test_id"
        assert conjunto.title == "Test Dataset"
        assert conjunto.slug == "test-dataset"
