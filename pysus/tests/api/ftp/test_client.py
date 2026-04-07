import pytest
import pathlib
from unittest.mock import MagicMock, patch
from datetime import datetime
from pysus.api.ftp.client import FTP


@pytest.fixture
def ftp_client():
    client = FTP()
    return client


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


def test_line_parser_with_formatter(ftp_client):
    def mock_formatter(name):
        return {"year": 2026, "state": "SC"}

    line = "03-09-26  04:30PM                12345 CIHASC2601.dbc"
    info = ftp_client._line_parser(line, formatter=mock_formatter)

    assert info["year"] == 2026
    assert info["state"] == "SC"


@pytest.mark.asyncio
async def test_connect_and_login(ftp_client):
    with patch("pysus.api.ftp.client.FTPLib") as mock_ftplib:
        mock_instance = mock_ftplib.return_value
        await ftp_client.login()

        mock_ftplib.assert_called_once_with(ftp_client.host)
        mock_instance.login.assert_called_once()


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
        await ftp_client._download_file(mock_file, pathlib.Path("test.dbc"))
        assert mock_connect.call_count >= 1


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
