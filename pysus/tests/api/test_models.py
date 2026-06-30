import hashlib
from collections.abc import AsyncGenerator, Callable
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from pydantic import ValidationError
from pysus import CACHEPATH
from pysus.api.extensions import Parquet
from pysus.api.models import BaseRemoteGroup  # noqa
from pysus.api.models import (
    BaseCompressedFile,
    BaseLocalFile,
    BaseRemoteClient,
    BaseRemoteDataset,
    BaseRemoteFile,
    BaseRemoteObject,
    BaseTabularFile,
)


class MockLocalFile(BaseLocalFile):
    type: str = "mock"

    async def load(self) -> bytes:
        return b"test content"

    async def stream(
        self,
        chunk_size: int = 10000,
    ) -> AsyncGenerator[Any, None]:
        yield b"test content"


class MockRemoteFile(BaseRemoteFile):
    type: str = "remote"

    @property
    def extension(self) -> str:
        return ".txt"

    @property
    def size(self) -> int:
        return 12

    @property
    def modify(self) -> datetime:
        return datetime(2026, 1, 1)

    async def _download(
        self,
        output: Path | None = None,
        callback: Callable[[int, int], None] | None = None,
    ) -> Path:
        if not output:
            raise ValueError()
        output.write_bytes(b"test content")
        return output


class MockTabularFile(BaseTabularFile):
    type: str = "tabular"

    @property
    def columns(self) -> list:
        return getattr(self, "_columns_val", [])

    @property
    def rows(self) -> int:
        return getattr(self, "_rows_val", 0)

    async def load(self) -> pd.DataFrame:
        return pd.DataFrame()

    async def stream(
        self,
        chunk_size: int = 10000,
    ) -> AsyncGenerator[pd.DataFrame, None]:
        for chunk in getattr(self, "_chunks", []):
            yield chunk


class MockCompressedFile(BaseCompressedFile):
    type: str = "compressed"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._members_list = ["member1.txt", "member2.txt"]
        self._member_data = {
            "member1.txt": b"content1",
            "member2.txt": b"content2",
        }

    async def load(self) -> bytes:
        return b""

    async def list_members(self) -> list[str]:
        return self._members_list

    async def open_member(self, member_name: str) -> Any:
        return self._member_data.get(member_name, b"")

    async def extract(
        self,
        target_dir: Path = CACHEPATH,
    ) -> list[BaseLocalFile]:
        return []


class MockRemoteGroup(BaseRemoteGroup):
    type: str = "group"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._files = None
        self._mock_files = []
        self._name_val = "test_group"

    @property
    def name(self) -> str:
        return self._name_val

    @property
    def long_name(self) -> str:
        return "Test Group"

    @property
    def description(self) -> str:
        return "A test group"

    async def _fetch_files(self) -> list[BaseRemoteFile]:
        return self._mock_files


class MockRemoteDataset(BaseRemoteDataset):
    type: str = "dataset"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._content = None
        self._mock_content = []
        self._name_val = "test_dataset"

    @property
    def name(self) -> str:
        return self._name_val

    @property
    def long_name(self) -> str:
        return "Test Dataset"

    @property
    def description(self) -> str:
        return "A test dataset"

    async def _fetch_content(self):
        return self._mock_content


MockRemoteFile.model_rebuild()
MockTabularFile.model_rebuild()
MockCompressedFile.model_rebuild()
MockRemoteGroup.model_rebuild()
MockRemoteDataset.model_rebuild()


# --- BaseFile ---


def test_base_file_str(tmp_path):
    path = tmp_path / "some_file.txt"
    path.write_text("hello")
    f = MockLocalFile(path=path)
    assert str(f) == "some_file.txt"


# --- BaseLocalFile ---


def test_base_local_file_name(tmp_path):
    path = tmp_path / "my_data.csv"
    path.write_text("a,b\n1,2")
    f = MockLocalFile(path=path)
    assert f.name == "my_data.csv"


def test_base_local_file_extension(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("a,b\n1,2")
    f = MockLocalFile(path=path)
    assert f.extension == ".csv"


def test_base_local_file_size(tmp_path):
    path = tmp_path / "data.bin"
    content = b"hello"
    path.write_bytes(content)
    f = MockLocalFile(path=path)
    assert f.size == len(content)


def test_base_local_file_modify(tmp_path):
    path = tmp_path / "data.txt"
    path.write_text("hello")
    f = MockLocalFile(path=path)
    assert isinstance(f.modify, datetime)


# --- BaseTabularFile.to_parquet ---


@pytest.mark.asyncio
async def test_to_parquet_no_output_path(tmp_path):
    tabular = MockTabularFile(path=tmp_path / "source.csv")
    tabular._chunks = [pd.DataFrame({"a": [1, 2, 3]})]
    tabular._rows_val = 3

    with patch(
        "pysus.api.extensions.ExtensionFactory.instantiate"
    ) as mock_inst:
        mock_inst.return_value = MagicMock(spec=Parquet)
        result = await tabular.to_parquet()
        assert isinstance(result, MagicMock)


@pytest.mark.asyncio
async def test_to_parquet_empty_chunk(tmp_path):
    tabular = MockTabularFile(path=tmp_path / "source.csv")
    tabular._chunks = [pd.DataFrame(), pd.DataFrame({"a": [1, 2, 3]})]
    tabular._rows_val = 3
    out = tmp_path / "out.parquet"

    with patch(
        "pysus.api.extensions.ExtensionFactory.instantiate"
    ) as mock_inst:
        mock_inst.return_value = MagicMock(spec=Parquet)
        result = await tabular.to_parquet(output_path=out)
        assert isinstance(result, MagicMock)


@pytest.mark.asyncio
async def test_to_parquet_null_schema(tmp_path):
    tabular = MockTabularFile(path=tmp_path / "source.csv")
    tabular._chunks = [pd.DataFrame({"a": [1], "b": [None]})]
    tabular._rows_val = 1
    out = tmp_path / "out.parquet"

    with patch(
        "pysus.api.extensions.ExtensionFactory.instantiate"
    ) as mock_inst:
        mock_inst.return_value = MagicMock(spec=Parquet)
        result = await tabular.to_parquet(output_path=out)
        assert isinstance(result, MagicMock)


@pytest.mark.asyncio
async def test_to_parquet_callback(tmp_path):
    tabular = MockTabularFile(path=tmp_path / "source.csv")
    tabular._chunks = [pd.DataFrame({"a": [1, 2, 3]})]
    tabular._rows_val = 3
    out = tmp_path / "out.parquet"
    callback = MagicMock()

    with patch(
        "pysus.api.extensions.ExtensionFactory.instantiate"
    ) as mock_inst:
        mock_inst.return_value = MagicMock(spec=Parquet)
        await tabular.to_parquet(output_path=out, callback=callback)
        callback.assert_called_once_with(3, 3)


@pytest.mark.asyncio
async def test_to_parquet_cleanup(tmp_path):
    tabular = MockTabularFile(path=tmp_path / "source.csv")
    tabular._chunks = [pd.DataFrame({"a": [1, 2, 3]})]
    tabular._rows_val = 3
    out = tmp_path / "out.parquet"

    with patch("pyarrow.parquet.ParquetWriter") as mock_writer_cls:
        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer
        with patch(
            "pysus.api.extensions.ExtensionFactory.instantiate"
        ) as mock_inst:
            mock_inst.return_value = MagicMock(spec=Parquet)
            await tabular.to_parquet(output_path=out)
        mock_writer.close.assert_called_once()


@pytest.mark.asyncio
async def test_to_parquet_value_error(tmp_path):
    tabular = MockTabularFile(path=tmp_path / "source.csv")
    tabular._chunks = [pd.DataFrame({"a": [1]})]
    tabular._rows_val = 1
    out = tmp_path / "out.parquet"

    with patch(
        "pysus.api.extensions.ExtensionFactory.instantiate"
    ) as mock_inst:
        mock_inst.return_value = "not_a_parquet"
        with pytest.raises(ValueError, match="Could not parse"):
            await tabular.to_parquet(output_path=out)


# --- BaseCompressedFile ---


@pytest.mark.asyncio
async def test_base_compressed_file_stream(tmp_path):
    path = tmp_path / "archive.zip"
    path.write_text("dummy")
    comp = MockCompressedFile(path=path)
    results = []
    async for member in comp.stream():
        results.append(member)
    assert results == [b"content1", b"content2"]


# --- SearchableMixin ---


def test_searchable_mixin_matches():
    obj = MagicMock()
    obj.year = 2024
    obj.month = 6
    mixin = BaseRemoteFile.__bases__[1]()
    assert mixin._matches(obj, year=2024, month=6) is True
    assert mixin._matches(obj, year=2025) is False
    assert mixin._matches(obj, extra_attr="missing") is False


# --- BaseRemoteFile ---


def test_base_remote_file_name():
    ds = MagicMock(spec=BaseRemoteDataset)
    f = MockRemoteFile(path="remote/path.txt", dataset=ds)
    assert f.name == "path.txt"


def test_base_remote_file_client():
    fake_client = MagicMock(spec=BaseRemoteClient)
    ds = MagicMock(spec=BaseRemoteDataset)
    ds.client = fake_client
    f = MockRemoteFile(path="remote/path.txt", dataset=ds)
    assert f.client is fake_client


def test_base_remote_file_year():
    ds = MagicMock(spec=BaseRemoteDataset)
    f = MockRemoteFile(path="r/p.txt", dataset=ds)
    assert f.year is None


def test_base_remote_file_month():
    ds = MagicMock(spec=BaseRemoteDataset)
    f = MockRemoteFile(path="r/p.txt", dataset=ds)
    assert f.month is None


def test_base_remote_file_state():
    ds = MagicMock(spec=BaseRemoteDataset)
    f = MockRemoteFile(path="r/p.txt", dataset=ds)
    assert f.state is None


@pytest.mark.asyncio
async def test_remote_file_download_default_cache(tmp_path):
    ds = MagicMock(spec=BaseRemoteDataset)
    remote = MockRemoteFile(path="remote/path.txt", dataset=ds)

    with patch("pysus.api.extensions.ExtensionFactory.instantiate") as mi:
        mock_local = MagicMock(spec=BaseLocalFile)
        mi.return_value = mock_local
        with patch("pysus.api.models.CACHEPATH", tmp_path):
            result = await remote.download()
            assert result == mock_local
            assert (tmp_path / "path.txt").exists()


@pytest.mark.asyncio
async def test_remote_file_download_output_dir(tmp_path):
    ds = MagicMock(spec=BaseRemoteDataset)
    remote = MockRemoteFile(path="remote/path.txt", dataset=ds)
    out_dir = tmp_path / "outdir"
    out_dir.mkdir()

    with patch("pysus.api.extensions.ExtensionFactory.instantiate") as mi:
        mock_local = MagicMock(spec=BaseLocalFile)
        mi.return_value = mock_local
        result = await remote.download(output=out_dir)
        assert result == mock_local
        assert (out_dir / "path.txt").exists()


# --- BaseRemoteObject ---


def test_base_remote_object_str():
    class NamedObj(BaseRemoteObject):
        type: str = "test"

        @property
        def name(self) -> str:
            return "my_name"

        @property
        def long_name(self) -> str:
            return "My Name"

        @property
        def description(self) -> str:
            return "Desc"

    obj = NamedObj()
    assert str(obj) == "my_name"


# --- BaseRemoteGroup ---


@pytest.mark.asyncio
async def test_base_remote_group_parent():
    ds = MagicMock(spec=BaseRemoteDataset)
    group = MockRemoteGroup(dataset=ds)
    assert group.parent is ds


@pytest.mark.asyncio
async def test_base_remote_group_files(tmp_path):
    ds = MagicMock(spec=BaseRemoteDataset)
    mock_files = [MagicMock(spec=BaseRemoteFile)]
    group = MockRemoteGroup(dataset=ds)
    group._mock_files = mock_files
    group._files = None

    files = await group.files
    assert files == mock_files
    assert group._files is mock_files


@pytest.mark.asyncio
async def test_base_remote_group_files_cached():
    ds = MagicMock(spec=BaseRemoteDataset)
    cached = [MagicMock(spec=BaseRemoteFile)]
    group = MockRemoteGroup(dataset=ds)
    group._files = cached

    files = await group.files
    assert files is cached


@pytest.mark.asyncio
async def test_base_remote_group_search_all():
    ds = MagicMock(spec=BaseRemoteDataset)
    f1 = MagicMock(spec=BaseRemoteFile, year=2024)
    f2 = MagicMock(spec=BaseRemoteFile, year=2025)
    group = MockRemoteGroup(dataset=ds)
    group._mock_files = [f1, f2]
    group._files = None

    result = await group.search()
    assert result == [f1, f2]


@pytest.mark.asyncio
async def test_base_remote_group_search_with_kwargs():
    ds = MagicMock(spec=BaseRemoteDataset)
    f1 = MagicMock(spec=BaseRemoteFile)
    f1.year = 2024
    f2 = MagicMock(spec=BaseRemoteFile)
    f2.year = 2025
    group = MockRemoteGroup(dataset=ds)
    group._mock_files = [f1, f2]
    group._files = None

    result = await group.search(year=2024)
    assert result == [f1]


# --- BaseRemoteDataset ---


@pytest.mark.asyncio
async def test_base_remote_dataset_search():
    client = MagicMock(spec=BaseRemoteClient)
    ds = MockRemoteDataset(client=client)
    ds._content = None

    f1 = MagicMock(spec=BaseRemoteFile)
    f1.year = 2024
    f2 = MagicMock(spec=BaseRemoteFile)
    f2.year = 2025

    group = MagicMock(spec=BaseRemoteGroup)
    group.search = AsyncMock(return_value=[f1])

    ds._mock_content = [group, f2]

    result = await ds.search(year=2024)
    assert result == [f1]
    group.search.assert_called_once_with(year=2024)


@pytest.mark.asyncio
async def test_base_remote_dataset_search_no_kwargs():
    client = MagicMock(spec=BaseRemoteClient)
    ds = MockRemoteDataset(client=client)
    ds._content = None

    f1 = MagicMock(spec=BaseRemoteFile)
    f1.year = 2024
    f2 = MagicMock(spec=BaseRemoteFile)
    f2.year = 2025

    ds._mock_content = [f1, f2]

    result = await ds.search()
    assert result == [f1, f2]


# --- Existing tests (unchanged) ---


@pytest.mark.asyncio
async def test_get_hash(tmp_path):
    path = tmp_path / "test_file.txt"
    content = b"test content"
    path.write_bytes(content)

    file_model = MockLocalFile(path=path)

    expected_hash = hashlib.sha256(content).hexdigest()
    generated_hash = await file_model.get_hash()

    assert generated_hash == expected_hash


@pytest.mark.asyncio
async def test_remote_file_download(tmp_path):
    mock_dataset = MagicMock(spec=BaseRemoteDataset)
    remote = MockRemoteFile(path="remote/path.txt", dataset=mock_dataset)
    dest = tmp_path / "downloaded.txt"

    result = await remote.download(output=dest)
    assert result.path == dest
    assert dest.exists()


def test_pydantic_validation():
    with pytest.raises(ValidationError):
        MockRemoteFile(path="missing_parent")
