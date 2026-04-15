from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from pysus.api.ftp.client import FTP
from pysus.api.ftp.models import Dataset, Directory, File, Group


@pytest.fixture
def mock_client():
    client = MagicMock(spec=FTP)
    client._list_directory = AsyncMock()
    client._download_file = AsyncMock()
    return client


@pytest.fixture
def mock_dataset(mock_client):
    dataset = MagicMock(spec=Dataset)
    dataset.client = mock_client
    dataset.formatter = lambda x: {}
    return dataset


@pytest.mark.asyncio
async def test_file_properties(mock_dataset):
    info = {
        "path": "/root/test.dbc",
        "name": "test.dbc",
        "size": 1000,
        "type": "file",
        "modify": datetime(2026, 1, 1),
        "year": 2026,
        "state": "SP",
        "group": {"name": "root", "long_name": "Test Group"},
    }

    file = File(
        path="/root/test.dbc",
        _info=info,
        type="file",
        dataset=mock_dataset,
    )

    assert file.name == "test.dbc"
    assert file.extension == ".dbc"
    assert file.size == 1000
    assert file.year == 2026
    assert file.state == "SP"
    assert file.group.name == "root"
    assert isinstance(file.modify, datetime)


@pytest.mark.asyncio
async def test_directory_load(mock_client, mock_dataset):
    mock_client._list_directory.return_value = [
        {"name": "subdir", "type": "dir", "path": "/root/subdir"},
        {
            "name": "file.dbc",
            "type": "file",
            "path": "/root/file.dbc",
            "size": 500,
            "modify": datetime.now(),
        },
    ]

    dr = Directory(path="/root", client=mock_client, dataset=mock_dataset)
    content = await dr.content

    assert len(content) == 2
    assert isinstance(content[0], Directory)
    assert isinstance(content[1], File)
    assert str(content[0].path) == "/root/subdir"
    assert str(content[1].path) == "/root/file.dbc"


@pytest.mark.asyncio
async def test_group_instantiation(mock_dataset):
    group = Group(
        path="/root/DC",
        dataset=mock_dataset,
        long_name="Dados Complementares",
        description="Desc",
    )

    assert group.name == "DC"
    assert group.long_name == "Dados Complementares"
    assert group.path == "/root/DC"


@pytest.mark.asyncio
async def test_dataset_fetch_content(mock_client):
    class TestDB(Dataset):
        @property
        def name(self):
            return "TEST"

        @property
        def long_name(self):
            return "Test DB"

        @property
        def description(self):
            return "Desc"

        def formatter(self, f):
            return {}

    db = TestDB(client=mock_client)
    root = Directory(path="/root", client=mock_client, dataset=db)
    db.paths = [root]
    db.group_definitions = {"SUB": "Subgroup Long Name"}

    mock_client._list_directory.return_value = [
        {"name": "SUB", "type": "dir", "path": "/root/SUB"},
        {"name": "OTHER", "type": "dir", "path": "/root/OTHER"},
        {"name": "file.dbc", "type": "file", "path": "/root/file.dbc"},
    ]

    content = await db.content

    assert len(content) == 3
    assert any(isinstance(c, Group) and c.name == "SUB" for c in content)
    assert any(isinstance(c, Directory) and c.name == "OTHER" for c in content)
    assert any(isinstance(c, File) for c in content)


@pytest.mark.asyncio
async def test_file_download_calls_client(mock_client, mock_dataset):
    file = File(
        path="/root/test.dbc",
        _info={"path": "/root/test.dbc", "name": "test.dbc"},
        type="file",
        dataset=mock_dataset,
    )

    dest = Path("/tmp/test.dbc")
    await file._download(output=dest)

    mock_client._download_file.assert_called_once_with(file, dest, None)
