import hashlib
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator, Callable, Optional, Union  # noqa
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError
from pysus.api.models import BaseRemoteGroup  # noqa
from pysus.api.models import BaseLocalFile, BaseRemoteDataset, BaseRemoteFile


class MockLocalFile(BaseLocalFile):
    type: str = "mock"

    async def load(self) -> bytes:
        return b"test content"

    async def stream(
        self,
        chunk_size: int = 1024,
    ) -> AsyncGenerator[bytes, None]:
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
        self, output: Path, callback: Optional[Callable[[int], None]] = None
    ) -> Path:
        output.write_bytes(b"test content")
        return output


MockRemoteFile.model_rebuild()


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
    remote = MockRemoteFile(path="remote/path.txt", parent=mock_dataset)
    dest = tmp_path / "downloaded.txt"

    result = await remote.download(output=dest)
    assert result.path == dest
    assert dest.exists()


def test_pydantic_validation():
    with pytest.raises(ValidationError):
        MockRemoteFile(path="missing_parent")
