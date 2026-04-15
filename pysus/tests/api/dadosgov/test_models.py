from unittest.mock import AsyncMock, MagicMock

import pytest
from pysus.api.dadosgov.client import Recurso
from pysus.api.dadosgov.models import File, Group


@pytest.fixture
def mock_client():
    from pysus.api.dadosgov.client import DadosGov

    client = MagicMock(spec=DadosGov)
    client._client = AsyncMock()
    return client


@pytest.fixture
def mock_recurso():
    return Recurso(
        id="1",
        titulo="Test Resource",
        link="http://example.com/test.csv",
        tamanho=1000,
        dataUltimaAtualizacaoArquivo="01/01/2026",
        nomeArquivo="test.csv",
    )


@pytest.fixture
def mock_dataset(mock_client):
    from pysus.api.dadosgov.databases import PNI

    dataset = PNI(client=mock_client)
    return dataset


class TestFile:
    @pytest.mark.asyncio
    async def test_file_creation(self, mock_recurso, mock_dataset):
        file = File(
            record=mock_recurso,
            dataset=mock_dataset,
            _metadata={"year": 2024, "month": 1, "state": "SP"},
        )

        assert file.record == mock_recurso
        assert file.extension == ".csv"
        assert file.size == 1000

    @pytest.mark.asyncio
    async def test_file_extension_from_url(self, mock_dataset):
        recurso = Recurso(
            id="1",
            titulo="Test",
            link="http://example.com/file.csv.zip",
            tamanho=100,
        )
        file = File(record=recurso, dataset=mock_dataset)

        assert file.extension == ".zip"

    @pytest.mark.asyncio
    async def test_file_year_from_metadata(self, mock_recurso, mock_dataset):
        file = File(
            record=mock_recurso,
            dataset=mock_dataset,
            _metadata={"year": 2024, "month": 1},
        )

        assert file.year == 2024
        assert file.month == 1

    @pytest.mark.asyncio
    async def test_file_state_from_metadata(self, mock_recurso, mock_dataset):
        file = File(
            record=mock_recurso,
            dataset=mock_dataset,
            _metadata={"state": "SP"},
        )

        assert file.state == "SP"

    @pytest.mark.asyncio
    async def test_file_modify_raises_if_none(self, mock_dataset):
        recurso = Recurso(
            id="1",
            titulo="Test",
            link="http://example.com/file.csv",
            tamanho=100,
        )
        file = File(record=recurso, dataset=mock_dataset)

        with pytest.raises(ValueError, match="modify date"):
            _ = file.modify


class TestGroup:
    @pytest.mark.asyncio
    async def test_group_properties(self, mock_client, mock_dataset):
        from pysus.api.dadosgov.client import ConjuntoDados

        conjunto = ConjuntoDados(
            id="1",
            titulo="Test Dataset",
            nome="test-dataset",
            recursos=[],
        )
        group = Group(record=conjunto, dataset=mock_dataset)

        assert group.name == "test-dataset"
        assert group.long_name == "Test Dataset"
        assert group.description == ""

    @pytest.mark.asyncio
    async def test_group_fetch_files(self, mock_client, mock_dataset):
        from pysus.api.dadosgov.client import ConjuntoDados, Recurso

        recurso = Recurso(
            id="1",
            titulo="Resource 1",
            link="http://example.com/1.csv",
            tamanho=100,
        )
        conjunto = ConjuntoDados(
            id="1",
            titulo="Test",
            nome="test",
            recursos=[recurso],
        )
        group = Group(record=conjunto, dataset=mock_dataset)

        files = await group._fetch_files()
        assert len(files) == 1
        assert isinstance(files[0], File)


@pytest.mark.asyncio
async def test_dataset_fetch_content(mock_client, mock_dataset):
    mock_dataset.ids = ["id1"]
    mock_dataset.formatter = lambda r, g: {"year": 2024}

    mock_client.get_dataset = AsyncMock(
        return_value=MagicMock(
            id="id1",
            title="Test",
            slug="test",
            resources=[],
        )
    )

    content = await mock_dataset._fetch_content()
    assert isinstance(content, list)
