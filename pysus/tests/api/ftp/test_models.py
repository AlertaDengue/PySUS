from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

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


def test_file_init_path_from_info(mock_dataset):
    info = {"path": "/root/test.dbc", "name": "test.dbc", "size": 1000}
    file = File(
        _info=info,
        type="file",
        dataset=mock_dataset,
    )
    assert file.path == Path("/root/test.dbc")


def test_file_repr(mock_dataset):
    file = File(
        path="/root/test.dbc",
        _info={"path": "/root/test.dbc", "name": "test.dbc"},
        type="file",
        dataset=mock_dataset,
    )
    assert repr(file) == "test.dbc"


def test_file_month(mock_dataset):
    info = {"path": "/root/test.dbc", "name": "test.dbc", "month": 6}
    file = File(
        path="/root/test.dbc",
        _info=info,
        type="file",
        dataset=mock_dataset,
    )
    assert file.month == 6


def test_file_modify_raises_value_error(mock_dataset):
    info = {"path": "/root/test.dbc", "name": "test.dbc"}
    file = File(
        path="/root/test.dbc",
        _info=info,
        type="file",
        dataset=mock_dataset,
    )
    with pytest.raises(ValueError, match="modify"):
        _ = file.modify


@pytest.mark.asyncio
async def test_file_download_no_output(mock_client, mock_dataset, tmp_path):
    file = File(
        path="/root/test.dbc",
        _info={"path": "/root/test.dbc", "name": "test.dbc"},
        type="file",
        dataset=mock_dataset,
    )
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    with patch("pysus.api.ftp.models.CACHEPATH", cache_dir):
        await file._download()
        mock_client._download_file.assert_called_once()
        args, _ = mock_client._download_file.call_args
        assert args[1] == cache_dir / "test.dbc"


@pytest.mark.asyncio
async def test_file_download_calls_client(mock_client, mock_dataset, tmp_path):
    file = File(
        path="/root/test.dbc",
        _info={"path": "/root/test.dbc", "name": "test.dbc"},
        type="file",
        dataset=mock_dataset,
    )

    dest = Path(tmp_path / "test.dbc")
    await file._download(output=dest)

    mock_client._download_file.assert_called_once_with(file, dest, None)


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
    assert Path(content[0].path) == Path("/root/subdir")
    assert Path(content[1].path) == Path("/root/file.dbc")


@pytest.mark.asyncio
async def test_directory_load_no_ftp_client():
    dr = Directory(path="/root/test", client=MagicMock())
    with pytest.raises(ValueError, match="no ftp client found"):
        await dr.load()


def test_directory_str():
    dr = Directory(path="/root/test")
    assert str(dr).replace("\\", "/") == "/root/test"


def test_directory_repr():
    dr = Directory(path="/root/test")
    assert repr(dr).replace("\\", "/") == "<Directory: /root/test>"


@pytest.mark.asyncio
async def test_group_instantiation(mock_dataset):
    group = Group(
        name="DC",
        path="/root/DC",
        dataset=mock_dataset,
        long_name="Dados Complementares",
        description="Desc",
    )

    assert group.name == "DC"
    assert group.long_name == "Dados Complementares"
    assert group.path == "/root/DC"


def test_group_description(mock_dataset):
    group = Group(
        name="TEST",
        path="/root/TEST",
        dataset=mock_dataset,
        long_name="Test Group",
        description="A test group description",
    )
    assert group.description == "A test group description"


@pytest.mark.asyncio
async def test_group_content(mock_client, mock_dataset):
    group = Group(
        name="TEST",
        path="/root/TEST",
        dataset=mock_dataset,
        long_name="Test Group",
        description="Test",
    )
    group._dir._content = [MagicMock(spec=Directory), MagicMock(spec=File)]
    group._dir.loaded = True
    content = await group.content
    assert len(content) == 2


@pytest.mark.asyncio
async def test_group_fetch_files(mock_client, mock_dataset):
    group = Group(
        name="TEST",
        path="/root/TEST",
        dataset=mock_dataset,
        long_name="Test Group",
        description="Test",
    )
    dir1 = MagicMock(spec=Directory)
    file1 = MagicMock(spec=File)
    group._dir._content = [dir1, file1]
    group._dir.loaded = True
    files = await group._fetch_files()
    assert len(files) == 1
    assert files[0] is file1


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
async def test_dataset_fetch_content_skips_non_file_non_dir(mock_client):
    class TestDB(Dataset):
        @property
        def name(self):
            return "TEST"

        @property
        def long_name(self):
            return "Test DB"

        @property
        def description(self):
            return "Testing"

        def formatter(self, f):
            return {}

    db = TestDB(client=mock_client)
    root = Directory(path="/root", client=mock_client, dataset=db)
    db.paths = [root]
    root._content = [MagicMock(spec=object)]
    root.loaded = True

    result = await db._fetch_content()
    assert len(result) == 0


@pytest.mark.asyncio
async def test_dataset_fetch_content_raises_runtime_error(mock_client):
    class TestDB(Dataset):
        @property
        def name(self):
            return "TEST"

        @property
        def long_name(self):
            return "Test DB"

        @property
        def description(self):
            return "Testing"

        def formatter(self, f):
            return {}

    db = TestDB(client=mock_client)
    fake_dir = MagicMock()
    db.paths = [fake_dir]
    with pytest.raises(RuntimeError, match="not instantiated"):
        await db._fetch_content()


def test_dataset_repr(mock_client):
    class TestDB(Dataset):
        @property
        def name(self):
            return "TEST"

        @property
        def long_name(self):
            return "Test DB"

        @property
        def description(self):
            return "Testing"

        def formatter(self, f):
            return {}

    db = TestDB(client=mock_client)
    assert repr(db) == "TEST"
