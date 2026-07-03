import pathlib
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from pysus.api.errors import ConnectionError, ParseError
from pysus.api.ftp.client import FTP


@pytest.fixture
def ftp_client():
    client = FTP()
    return client


def test_name_property(ftp_client):
    assert ftp_client.name == "FTP"


def test_long_name_property(ftp_client):
    assert ftp_client.long_name == "Pysus FTP Client"


def test_description_property(ftp_client):
    assert isinstance(ftp_client.description, str)
    assert len(ftp_client.description) > 0


def test_line_parser_file(ftp_client):
    line = "03-09-26  04:30PM                12345 filename.dbc"
    info = ftp_client._line_parser(line)

    assert info["name"] == "filename.dbc"
    assert info["size"] == 12345
    assert info["type"] == "file"
    assert isinstance(info["modify"], datetime)


def test_line_parser_directory(ftp_client):
    line = "03-09-26  04:30PM       <DIR>          DADOS"
    info = ftp_client._line_parser(line)

    assert info["name"] == "DADOS"
    assert info["size"] == 0
    assert info["type"] == "dir"


def test_line_parser_with_formatter_on_directory(ftp_client):
    def mock_formatter(name):
        return {"year": 2026, "state": "SC"}

    line = "03-09-26  04:30PM       <DIR>          DADOS"
    info = ftp_client._line_parser(line, formatter=mock_formatter)

    assert info["type"] == "dir"
    assert info["year"] is None


def test_line_parser_with_formatter(ftp_client):
    def mock_formatter(name):
        return {"year": 2026, "state": "SC"}

    line = "03-09-26  04:30PM                12345 CIHASC2601.dbc"
    info = ftp_client._line_parser(line, formatter=mock_formatter)

    assert info["year"] == 2026
    assert info["state"] == "SC"


def test_line_parser_invalid_line(ftp_client):
    with pytest.raises(ParseError, match="Invalid FTP line"):
        ftp_client._line_parser("only three")


def test_line_parser_invalid_date(ftp_client):
    info = ftp_client._line_parser("invalid-date invalid-time <DIR> DADOS")
    assert info["name"] == "DADOS"
    assert info["type"] == "dir"
    assert isinstance(info["modify"], datetime)


@pytest.mark.asyncio
async def test_close_when_not_connected(ftp_client):
    ftp_client._ftp = None
    await ftp_client.close()
    assert ftp_client._ftp is None


@pytest.mark.asyncio
async def test_connect_when_already_connected(ftp_client):
    mock_ftp = MagicMock()
    ftp_client._ftp = mock_ftp
    await ftp_client.connect()
    mock_ftp.quit.assert_not_called()
    mock_ftp.close.assert_not_called()


@pytest.mark.asyncio
async def test_close_normal(ftp_client):
    mock_ftp = MagicMock()
    ftp_client._ftp = mock_ftp
    await ftp_client.close()
    mock_ftp.quit.assert_called_once()
    assert ftp_client._ftp is None


@pytest.mark.asyncio
async def test_close_quit_raises_exception(ftp_client):
    mock_ftp = MagicMock()
    mock_ftp.quit.side_effect = Exception("connection error")
    ftp_client._ftp = mock_ftp
    await ftp_client.close()
    mock_ftp.quit.assert_called_once()
    mock_ftp.close.assert_called_once()
    assert ftp_client._ftp is None


@pytest.mark.asyncio
async def test_connect_and_login(ftp_client):
    with patch("pysus.api.ftp.client.FTPLib") as mock_ftplib:
        mock_instance = mock_ftplib.return_value
        await ftp_client.login()

        mock_ftplib.assert_called_once_with(
            ftp_client.host, timeout=ftp_client.timeout
        )
        mock_instance.login.assert_called_once()


@pytest.mark.asyncio
async def test_datasets_raises_connection_error(ftp_client):
    ftp_client._ftp = None
    with pytest.raises(ConnectionError, match="not connected"):
        await ftp_client.datasets()


@pytest.mark.asyncio
async def test_download_file_reconnects_on_failure(ftp_client):
    mock_ftp_internal = MagicMock()
    mock_ftp_internal.voidcmd.side_effect = [BrokenPipeError, None]
    ftp_client._ftp = mock_ftp_internal

    mock_file = MagicMock()
    mock_file.path = "remote/path.dbc"

    with (
        patch("pysus.api.ftp.client.FTP.connect") as mock_connect,
        patch("builtins.open", MagicMock()),
    ):
        await ftp_client.download(mock_file, pathlib.Path("test.dbc"))
        assert mock_connect.call_count >= 1


@pytest.mark.asyncio
async def test_download_file_with_callback(ftp_client):
    mock_ftp_internal = MagicMock()
    ftp_client._ftp = mock_ftp_internal

    mock_file = MagicMock()
    mock_file.path = "remote/path.dbc"

    callback = MagicMock()

    def simulate_retrbinary(cmd, cb):
        cb(b"chunk_data")

    mock_ftp_internal.retrbinary.side_effect = simulate_retrbinary

    with patch("builtins.open", MagicMock()):
        await ftp_client.download(
            mock_file, pathlib.Path("test.dbc"), callback=callback
        )
        callback.assert_called_once()


@pytest.mark.asyncio
async def test_download_file_without_callback(ftp_client):
    mock_ftp_internal = MagicMock()
    ftp_client._ftp = mock_ftp_internal

    mock_file = MagicMock()
    mock_file.path = "remote/path.dbc"

    def simulate_retrbinary(cmd, cb):
        cb(b"chunk_data")

    mock_ftp_internal.retrbinary.side_effect = simulate_retrbinary

    with patch("builtins.open", MagicMock()):
        await ftp_client.download(mock_file, pathlib.Path("test.dbc"))


@pytest.mark.asyncio
async def test_list_directory_calls_ftp_methods(ftp_client):
    mock_ftp_internal = MagicMock()
    ftp_client._ftp = mock_ftp_internal

    with patch.object(ftp_client, "_line_parser") as mock_parser:
        mock_parser.return_value = {"name": "test", "type": "file"}

        def simulate_retrlines(cmd, callback):
            callback("03-09-26  04:30PM                12345 test.dbc")

        mock_ftp_internal.retrlines.side_effect = simulate_retrlines

        await ftp_client._list_directory("/test/path")

        mock_ftp_internal.cwd.assert_called_once_with("/test/path")
        mock_ftp_internal.retrlines.assert_called_once()
