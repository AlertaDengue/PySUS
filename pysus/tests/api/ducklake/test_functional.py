"""Tests for pysus.api.ducklake.functional (HTTP/S3 download utilities)."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from pysus.api.ducklake.functional import download_http, download_s3


@pytest.mark.asyncio
async def test_download_http_success(tmp_path):
    local = tmp_path / "test.bin"
    content = b"hello world"

    mock_response = MagicMock()
    mock_response.headers = {"Content-Length": str(len(content))}

    mock_client = MagicMock()
    mock_client.__aenter__.return_value = mock_client

    async def fake_aiter_bytes(**kwargs):
        yield content

    mock_response.aiter_bytes = fake_aiter_bytes

    mock_stream = MagicMock()
    mock_stream.__aenter__.return_value = mock_response
    mock_client.stream.return_value = mock_stream

    with patch("httpx.AsyncClient", return_value=mock_client):
        await download_http("remote/path", local)

    assert local.read_bytes() == content


@pytest.mark.asyncio
async def test_download_http_retry_on_remote_protocol_error(tmp_path):
    local = tmp_path / "test.bin"
    call_count = [0]
    final_content = b"success after retry"

    mock_client = MagicMock()
    mock_client.__aenter__.return_value = mock_client

    def make_response():
        call_count[0] += 1
        if call_count[0] <= 2:
            raise httpx.RemoteProtocolError("peer closed")
        r = MagicMock()
        r.headers = {"Content-Length": str(len(final_content))}
        r.raise_for_status = MagicMock()

        async def fake_bytes(**kwargs):
            yield final_content

        r.aiter_bytes = fake_bytes
        return r

    class FailThenSucceedCtx:
        async def __aenter__(self):
            return make_response()

        async def __aexit__(self, *a):
            pass

    mock_client.stream.return_value = FailThenSucceedCtx()

    with patch("httpx.AsyncClient", return_value=mock_client):
        with patch("httpx.Timeout", return_value=httpx.Timeout(5.0)):
            with patch("httpx.Limits", return_value=httpx.Limits()):
                with patch(
                    "pysus.api.ducklake.functional.sleep",
                    new_callable=AsyncMock,
                ):
                    await download_http("remote/path", local)

    assert local.read_bytes() == final_content
    assert call_count[0] == 3


@pytest.mark.asyncio
async def test_download_http_cleanup_partial_on_error(tmp_path):
    local = tmp_path / "test.bin"
    local.write_text("partial")

    mock_client = MagicMock()
    mock_client.__aenter__.return_value = mock_client

    class ErrorCtx:
        async def __aenter__(self):
            raise httpx.ConnectError("connection refused")

        async def __aexit__(self, *a):
            pass

    mock_client.stream.return_value = ErrorCtx()

    with patch("httpx.AsyncClient", return_value=mock_client):
        with patch("httpx.Timeout", return_value=httpx.Timeout(5.0)):
            with patch("httpx.Limits", return_value=httpx.Limits()):
                with patch(
                    "pysus.api.ducklake.functional.sleep",
                    new_callable=AsyncMock,
                ):
                    with pytest.raises(httpx.ConnectError):
                        await download_http("remote/path", local)

    assert not local.exists()


@pytest.mark.asyncio
async def test_download_http_retry_on_http_error(tmp_path):
    local = tmp_path / "test.bin"
    call_count = [0]
    final_content = b"data"

    mock_client = MagicMock()
    mock_client.__aenter__.return_value = mock_client

    def make_response():
        call_count[0] += 1
        if call_count[0] <= 2:
            raise httpx.HTTPStatusError(
                "500", request=MagicMock(), response=MagicMock()
            )
        r = MagicMock()
        r.headers = {"Content-Length": str(len(final_content))}
        r.raise_for_status = MagicMock()

        async def fake_bytes(**kwargs):
            yield final_content

        r.aiter_bytes = fake_bytes
        return r

    class RetryCtx:
        async def __aenter__(self):
            return make_response()

        async def __aexit__(self, *a):
            pass

    mock_client.stream.return_value = RetryCtx()

    with patch("httpx.AsyncClient", return_value=mock_client):
        with patch("httpx.Timeout", return_value=httpx.Timeout(5.0)):
            with patch("httpx.Limits", return_value=httpx.Limits()):
                with patch(
                    "pysus.api.ducklake.functional.sleep",
                    new_callable=AsyncMock,
                ):
                    await download_http("remote/path", local)

    assert local.read_bytes() == final_content


@pytest.mark.asyncio
async def test_download_http_callback(tmp_path):
    local = tmp_path / "test.bin"
    content = b"abc"
    progress = []

    mock_response = MagicMock()
    mock_response.headers = {"Content-Length": str(len(content))}
    mock_client = MagicMock()
    mock_client.__aenter__.return_value = mock_client

    async def fake_aiter_bytes(**kwargs):
        yield content

    mock_response.aiter_bytes = fake_aiter_bytes
    mock_stream = MagicMock()
    mock_stream.__aenter__.return_value = mock_response
    mock_client.stream.return_value = mock_stream

    with patch("httpx.AsyncClient", return_value=mock_client):
        await download_http(
            "remote/path", local, callback=lambda d, t: progress.append((d, t))
        )

    assert len(progress) > 0
    assert progress[-1] == (3, 3)


@pytest.mark.asyncio
async def test_download_s3_success(tmp_path):
    local = tmp_path / "test.bin"

    def fake_download_file(Bucket, Key, Filename, Callback=None):
        Path(Filename).write_text("s3data")

    with patch("boto3.client") as mock_boto:
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {"ContentLength": 6}
        mock_s3.download_file.side_effect = fake_download_file
        mock_boto.return_value = mock_s3
        await download_s3("remote/key", local)

    assert local.read_text() == "s3data"


@pytest.mark.asyncio
async def test_download_s3_callback(tmp_path):
    local = tmp_path / "test.bin"
    progress = []

    def fake_download_file(Bucket, Key, Filename, Callback=None):
        Path(Filename).write_text("s3data")
        if Callback:
            Callback(6)

    with patch("boto3.client") as mock_boto:
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {"ContentLength": 6}
        mock_s3.download_file.side_effect = fake_download_file
        mock_boto.return_value = mock_s3

        def cb(d, t):
            progress.append((d, t))

        await download_s3("remote/key", local, callback=cb)

    assert len(progress) > 0


@pytest.mark.asyncio
async def test_download_s3_head_error_fallback(tmp_path):
    local = tmp_path / "test.bin"

    with patch("boto3.client") as mock_boto:
        mock_s3 = MagicMock()
        mock_s3.head_object.side_effect = Exception("head failed")

        def fake_download_file(Bucket, Key, Filename, Callback=None):
            Path(Filename).write_text("s3data")

        mock_s3.download_file.side_effect = fake_download_file
        mock_boto.return_value = mock_s3
        await download_s3("remote/key", local)

    assert local.read_text() == "s3data"


@pytest.mark.asyncio
async def test_download_http_unlink_os_error_swallowed(tmp_path):
    """OSError during cleanup unlink is silently ignored."""
    local = tmp_path / "test.bin"
    local.write_text("partial")

    mock_client = MagicMock()
    mock_client.__aenter__.return_value = mock_client

    class ErrorCtx:
        async def __aenter__(self):
            raise httpx.ConnectError("connection failed")

        async def __aexit__(self, *a):
            pass

    mock_client.stream.return_value = ErrorCtx()

    with patch("httpx.AsyncClient", return_value=mock_client):
        with patch("httpx.Timeout", return_value=httpx.Timeout(5.0)):
            with patch("httpx.Limits", return_value=httpx.Limits()):
                with patch(
                    "pysus.api.ducklake.functional.sleep",
                    new_callable=AsyncMock,
                ):
                    with patch("pathlib.Path.unlink", side_effect=OSError):
                        with pytest.raises(httpx.ConnectError):
                            await download_http("remote/path", local)
