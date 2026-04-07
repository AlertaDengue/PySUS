import pytest
from unittest.mock import MagicMock
from pysus.api.ftp.databases import AVAILABLE_DATABASES
from pysus.api.ftp.client import FTP


@pytest.fixture
def mock_client():
    client = MagicMock(spec=FTP)
    return client


@pytest.mark.asyncio
async def test_database_metadata(mock_client):
    for db_class in AVAILABLE_DATABASES:
        db = db_class(client=mock_client)

        assert db.name is not None
        assert db.long_name is not None
        assert db.description is not None
        assert len(db.paths) > 0


@pytest.mark.parametrize("db_class", AVAILABLE_DATABASES)
def test_formatter_returns_valid_structure(db_class, mock_client):
    db = db_class(client=mock_client)

    test_files = {
        "CIHA": "CIHAAC1101.dbc",
        "CNES": "DCAC0508.dbc",
        "SINASC": "DNAC1996.DBC",
        "SIM": "DOAC1996.dbc",
        "PNI": "CPNIAC00.DBF",
        "IBGE": "POPBR00.zip",
        "SIA": "PAAC0001.dbc",
        "SIH": "RDAC0001.dbc",
        "SINAN": "ACBIBR06.dbc",
    }

    filename = test_files.get(db.name, "TEST0000.dbc")
    metadata = db.formatter(filename)

    assert isinstance(metadata, dict)
    assert "year" in metadata
    if db.name != "IBGE":
        assert "group" in metadata


@pytest.mark.asyncio
async def test_ftp_datasets_instantiation():
    client = FTP()
    client._ftp = MagicMock()

    databases = await client.datasets()
    assert len(databases) == len(AVAILABLE_DATABASES)

    for db in databases:
        assert db.client == client


@pytest.mark.asyncio
async def test_ciha_search_logic(mock_client):
    db = AVAILABLE_DATABASES[0](client=mock_client)

    res = db.formatter("CIHAAC1101.dbc")
    assert res["state"] == "AC"
    assert res["year"] == 2011
    assert res["month"] == 1
    assert res["group"]["name"] == "CIHA"
