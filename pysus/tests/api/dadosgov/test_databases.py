from unittest.mock import MagicMock

import pytest
from pysus.api.dadosgov.databases import AVAILABLE_DATABASES


@pytest.fixture
def mock_client():
    from pysus.api.dadosgov.client import DadosGov

    client = DadosGov()
    client._client = MagicMock()
    return client


@pytest.mark.asyncio
@pytest.mark.parametrize("db_class", AVAILABLE_DATABASES)
async def test_database_metadata(mock_client, db_class):
    db = db_class(client=mock_client)

    assert db.name is not None
    assert db.long_name is not None
    assert db.description is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("db_class", AVAILABLE_DATABASES)
async def test_database_ids_are_valid(mock_client, db_class):
    db = db_class(client=mock_client)

    assert db.ids is not None
    assert len(db.ids) > 0
    assert all(isinstance(id, str) for id in db.ids)


class TestFormatters:
    def test_cnes_formatter_raises(self, mock_client):
        from pysus.api.dadosgov.databases import CNES

        db = CNES(client=mock_client)
        with pytest.raises(NotImplementedError):
            db.formatter("test.csv")

    def test_pni_formatter_raises(self, mock_client):
        from pysus.api.dadosgov.databases import PNI

        db = PNI(client=mock_client)
        with pytest.raises(NotImplementedError):
            db.formatter("test.csv")


@pytest.mark.asyncio
async def test_available_databases_count(mock_client):
    assert len(AVAILABLE_DATABASES) > 0


@pytest.mark.asyncio
async def test_cnes_instantiation(mock_client):
    from pysus.api.dadosgov.databases import CNES

    db = CNES(client=mock_client)
    assert db.name == "CNES"
    assert db.long_name == "Cadastro Nacional de Estabelecimentos de Saúde"
    assert len(db.ids) > 0


@pytest.mark.asyncio
async def test_pni_instantiation(mock_client):
    from pysus.api.dadosgov.databases import PNI

    db = PNI(client=mock_client)
    assert db.name == "PNI"
    assert db.long_name == "Programa Nacional de Imunizações"
    assert len(db.ids) > 0
