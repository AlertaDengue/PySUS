import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, AsyncGenerator, Optional

from .models import BaseLocalFile, BaseTabularFile, BaseRemoteFile


class MockLocalFile(BaseLocalFile):
    type: str = "mock"

    async def load(self) -> str:
        return self.path.read_text()

    async def stream(
        self, chunk_size: Optional[int] = None
    ) -> AsyncGenerator[str, None]:
        content = self.path.read_text()
        yield content


class MockTabularFile(BaseTabularFile):
    type: str = "tabular"

    @property
    def columns(self) -> List[str]:
        return ["col1", "col2"]

    @property
    def rows(self) -> int:
        return 2

    async def load(self) -> pd.DataFrame:
        return pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

    async def stream(self, chunk_size: int = 10) -> AsyncGenerator[pd.DataFrame, None]:
        yield await self.load()


class MockRemoteFile(BaseRemoteFile):
    type: str = "remote"
    basename: str = "test.txt"

    async def _download(self, output: Path) -> Path:
        output.write_text("downloaded content")
        return output


@pytest.fixture
def temp_file(tmp_path):
    p = tmp_path / "test_file.txt"
    p.write_text("pysus test content")
    return p


@pytest.mark.asyncio
async def test_base_local_file_metadata(temp_file):
    file_model = MockLocalFile(path=temp_file)

    assert file_model.extension == ".txt"
    assert file_model.size > 0
    assert isinstance(file_model.modify, datetime)
    assert str(file_model) == "test_file.txt"


@pytest.mark.asyncio
async def test_get_hash(temp_file):
    file_model = MockLocalFile(path=temp_file)
    expected_hash = "7737c35593c6609f3e49339e162093f1d326922da19f2a2491136b69a68c072e"

    generated_hash = await file_model.get_hash()
    assert generated_hash == expected_hash


@pytest.mark.asyncio
async def test_tabular_file_properties(tmp_path):
    p = tmp_path / "table.csv"
    p.write_text("col1,col2\n1,3\n2,4")

    tabular = MockTabularFile(path=p)
    assert tabular.columns == ["col1", "col2"]
    assert tabular.rows == 2

    df = await tabular.load()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2


@pytest.mark.asyncio
async def test_remote_file_download(tmp_path):
    remote = MockRemoteFile(type="remote")
    download_dir = tmp_path / "downloads"

    local_file = await remote.download(output=download_dir)

    assert local_file.path.exists()
    assert local_file.path.name == "test.txt"
    assert local_file.path.read_text() == "downloaded content"


def test_pydantic_validation():
    with pytest.raises(ValueError):
        MockLocalFile(path="not-a-path-object")
