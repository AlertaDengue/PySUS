from unittest.mock import MagicMock

import pytest
from pysus.api.ftp.client import FTP
from pysus.api.ftp.databases import (
    AVAILABLE_DATABASES,
    CIHA,
    CNES,
    IBGEDATASUS,
    PNI,
    SIA,
    SIH,
    SIM,
    SINAN,
    SINASC,
)


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


def test_ciha_formatter_exception(mock_client):
    db = CIHA(client=mock_client)
    result = db.formatter("A")
    assert result == {"group": None, "state": None, "year": None, "month": None}


def test_cnes_formatter_exception(mock_client):
    db = CNES(client=mock_client)
    result = db.formatter("A")
    assert result == {"group": None, "state": None, "year": None, "month": None}


def test_sinasc_formatter_exception(mock_client):
    db = SINASC(client=mock_client)
    result = db.formatter("A")
    assert result == {"group": None, "state": None, "year": None}


def test_sim_formatter_cid9(mock_client):
    db = SIM(client=mock_client)
    result = db.formatter("CID9DOAC96.dbc")
    assert result["state"] == "AC"
    assert result["year"] == 1996


def test_sim_formatter_exception(mock_client):
    db = SIM(client=mock_client)
    result = db.formatter("A")
    assert result == {"group": None, "state": None, "year": None}


def test_pni_formatter_exception(mock_client):
    db = PNI(client=mock_client)
    result = db.formatter("A")
    assert result == {"group": None, "state": None, "year": None}


def test_ibge_formatter_proj(mock_client):
    db = IBGEDATASUS(client=mock_client)
    result = db.formatter("PROJBR00.zip")
    assert result["year"] == 2000
    assert result["group"]["name"] == "PROJ"


def test_ibge_formatter_exception(mock_client):
    db = IBGEDATASUS(client=mock_client)
    result = db.formatter("A")
    assert result == {"group": None, "year": None}


def test_sia_formatter_group_not_in_definitions(mock_client):
    db = SIA(client=mock_client)
    result = db.formatter("ZZAC0001.dbc")
    assert result["group"] is None


def test_sia_formatter_exception(mock_client):
    db = SIA(client=mock_client)
    result = db.formatter("A")
    assert result == {"group": None, "state": None, "year": None, "month": None}


def test_sih_formatter_exception(mock_client):
    db = SIH(client=mock_client)
    result = db.formatter("A")
    assert result == {"group": None, "state": None, "year": None, "month": None}


def test_sinan_formatter_src(mock_client):
    db = SINAN(client=mock_client)
    result = db.formatter("SRCBR06.dbc")
    assert result["group"]["name"] == "SRC"


def test_sinan_formatter_leibr22(mock_client):
    db = SINAN(client=mock_client)
    result = db.formatter("LEIBR22.dbc")
    assert result["group"]["name"] == "LEIV"


def test_sinan_formatter_lerbr19(mock_client):
    db = SINAN(client=mock_client)
    result = db.formatter("LERBR19.dbc")
    assert result["group"]["name"] == "LERD"


def test_sinan_formatter_exception(mock_client):
    db = SINAN(client=mock_client)
    result = db.formatter("A")
    assert result == {"group": None, "year": None}
