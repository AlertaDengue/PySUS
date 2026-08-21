"""Tests for pysus.api.partial — partial download recovery."""

import pytest
from pysus.api.partial import PartialDownload


class MockStreamResponse:
    """Mock httpx streaming response."""

    def __init__(self, data: bytes, status: int = 200):
        self.data = data
        self.status_code = status
        self.headers = {"content-length": str(len(data))}
        if status == 206:
            total = len(data) + 100
            self.headers["content-range"] = f"bytes 0-{total - 1}/{total}"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def aiter_bytes(self, chunk_size: int = 65536):
        for i in range(0, len(self.data), chunk_size):
            yield self.data[i : i + chunk_size]

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


class MockStreamCM:
    """Async context manager that yields a MockStreamResponse."""

    def __init__(self, data: bytes, status: int = 200):
        self.resp = MockStreamResponse(data, status)

    async def __aenter__(self):
        return self.resp

    async def __aexit__(self, *args):
        pass


class MockClient:
    """Mock httpx.AsyncClient."""

    def __init__(self, data: bytes, status: int = 200):
        self.data = data
        self.status = status

    def stream(self, method: str, url: str, headers=None):
        return MockStreamCM(self.data, self.status)


class TestPartialDownload:
    def test_exists_false(self, tmp_path):
        pd = PartialDownload(tmp_path / "test.bin")
        assert not pd.exists()

    def test_exists_true(self, tmp_path):
        dest = tmp_path / "test.bin"
        partial = dest.with_suffix(dest.suffix + ".partial")
        partial.write_bytes(b"hello")
        pd = PartialDownload(dest)
        assert pd.exists()

    def test_size(self, tmp_path):
        dest = tmp_path / "test.bin"
        partial = dest.with_suffix(dest.suffix + ".partial")
        partial.write_bytes(b"hello world")
        pd = PartialDownload(dest)
        assert pd.size() == 11

    def test_size_zero_when_not_exists(self, tmp_path):
        pd = PartialDownload(tmp_path / "test.bin")
        assert pd.size() == 0

    @pytest.mark.asyncio
    async def test_start_downloads_file(self, tmp_path):
        pd = PartialDownload(tmp_path / "test.bin")
        client = MockClient(b"file content here")
        result = await pd.start("http://example.com/test.bin", client)
        assert result.exists()
        assert result.read_bytes() == b"file content here"

    @pytest.mark.asyncio
    async def test_start_renames_from_partial(self, tmp_path):
        pd = PartialDownload(tmp_path / "sub" / "test.bin")
        client = MockClient(b"data")
        await pd.start("http://example.com/test.bin", client)
        assert not pd.partial_path.exists()
        assert pd.dest_path.exists()

    @pytest.mark.asyncio
    async def test_resume_fresh_when_no_partial(self, tmp_path):
        pd = PartialDownload(tmp_path / "test.bin")
        client = MockClient(b"data")
        result = await pd.resume("http://example.com/test.bin", client)
        assert result.exists()

    @pytest.mark.asyncio
    async def test_resume_uses_range_header(self, tmp_path):
        dest = tmp_path / "test.bin"
        partial = dest.with_suffix(dest.suffix + ".partial")
        partial.write_bytes(b"existing_")
        pd = PartialDownload(dest)

        resumed_data = b"rest_of_file"
        client = MockClient(resumed_data, status=206)
        result = await pd.resume("http://example.com/test.bin", client)
        assert result.exists()

    @pytest.mark.asyncio
    async def test_callback_called(self, tmp_path):
        pd = PartialDownload(tmp_path / "test.bin")
        client = MockClient(b"data")
        calls = []

        def cb(downloaded, total):
            calls.append((downloaded, total))

        await pd.start("http://example.com/test.bin", client, cb)
        assert len(calls) > 0
