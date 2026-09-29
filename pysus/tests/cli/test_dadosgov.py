"""Tests for pysus.cli.dadosgov CLI commands (Phase 2.2)."""

import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pysus.cli import app
from typer.testing import CliRunner

runner = CliRunner()


class TestDadosGovList:
    def test_list_all_datasets(self):
        result = runner.invoke(app, ["dadosgov", "list"])
        assert result.exit_code == 0
        assert "SINAN" in result.output
        assert "Total:" in result.output


class TestDadosGovSearch:
    def test_search_exact_match(self):
        result = runner.invoke(app, ["dadosgov", "search", "SINAN"])
        assert result.exit_code == 0
        assert "SINAN" in result.output

    def test_search_no_match(self):
        result = runner.invoke(app, ["dadosgov", "search", "xyzzy"])
        assert result.exit_code == 1
        assert "No" in result.output


class TestDadosGovShow:
    def test_show_known_dataset(self):
        result = runner.invoke(app, ["dadosgov", "show", "sinan"])
        assert result.exit_code == 0
        assert "Dataset:" in result.output
        assert "SINAN" in result.output

    def test_show_unknown_dataset(self):
        result = runner.invoke(app, ["dadosgov", "show", "FAKE"])
        assert result.exit_code == 1
        assert "not found" in result.output


class TestDadosGovDownload:
    def test_download_no_token(self):
        result = runner.invoke(app, ["dadosgov", "download", "SINAN"])
        assert result.exit_code == 1
        assert "token" in result.output.lower()

    def test_download_bad_slug(self):
        os.environ["DADOSGOV_TOKEN"] = "fake_token"
        try:
            result = runner.invoke(app, ["dadosgov", "download", "FAKE"])
            assert result.exit_code == 1
            assert "not found" in result.output
        finally:
            del os.environ["DADOSGOV_TOKEN"]


class _FakeDataset:
    """A DadosGov dataset stand-in exposing the real search() contract."""

    def __init__(self, files):
        self._files = files

    async def search(self, **kwargs):
        return self._files

    def __repr__(self) -> str:
        return "<fake SINAN dataset>"


# The CLI identifies the dataset by class name, so the stand-in needs one.
_FakeDataset.__name__ = "SINAN"


class _FakeClient:
    def __init__(self, files):
        self.connect = AsyncMock()
        self.close = AsyncMock()
        self.datasets = AsyncMock(return_value=[_FakeDataset(files)])
        self.downloaded = []
        self.download_awaits = 0

    async def download(self, file, output) -> Path:
        self.downloaded.append((str(file.path), Path(output)))
        self.download_awaits += 1
        return Path(output)


def _fake_file(path: str) -> MagicMock:
    f = MagicMock()
    f.path = path
    return f


class TestDadosGovDownloadWorks:
    """The download command must use the token and real model API."""

    @pytest.fixture
    def files(self):
        return [
            _fake_file(
                "https://dados.gov.br/dados/cidadaos/SINAN/2024/" "DENGBR24.csv"
            ),
            _fake_file(
                "https://dados.gov.br/dados/cidadaos/SINAN/2024/" "CHIKBR24.csv"
            ),
        ]

    def test_env_token_is_passed_to_connect(self, files, tmp_path):
        client = _FakeClient(files)
        os.environ["DADOSGOV_TOKEN"] = "env_token"
        try:
            with patch(
                "pysus.api.dadosgov.client.DadosGov", return_value=client
            ):
                result = runner.invoke(
                    app,
                    [
                        "dadosgov",
                        "download",
                        "SINAN",
                        "-o",
                        str(tmp_path),
                    ],
                )
        finally:
            del os.environ["DADOSGOV_TOKEN"]

        assert result.exit_code == 0, result.output
        client.connect.assert_awaited_once_with(token="env_token")

    def test_flag_token_takes_precedence(self, files, tmp_path):
        client = _FakeClient(files)
        os.environ["DADOSGOV_TOKEN"] = "env_token"
        try:
            with patch(
                "pysus.api.dadosgov.client.DadosGov", return_value=client
            ):
                result = runner.invoke(
                    app,
                    [
                        "dadosgov",
                        "download",
                        "SINAN",
                        "-t",
                        "flag_token",
                        "-o",
                        str(tmp_path),
                    ],
                )
        finally:
            del os.environ["DADOSGOV_TOKEN"]

        assert result.exit_code == 0, result.output
        client.connect.assert_awaited_once_with(token="flag_token")

    def test_download_writes_file_not_directory(self, files, tmp_path):
        client = _FakeClient(files)
        with patch("pysus.api.dadosgov.client.DadosGov", return_value=client):
            result = runner.invoke(
                app,
                [
                    "dadosgov",
                    "download",
                    "SINAN",
                    "-t",
                    "tok",
                    "-o",
                    str(tmp_path),
                ],
            )

        assert result.exit_code == 0, result.output
        assert client.download_awaits == 2
        # Each transfer must target a real file inside out_dir, not the
        # directory itself, which open() would refuse.
        for _, out in client.downloaded:
            assert out.parent == tmp_path
            assert out.is_absolute()
        names = sorted(out.name for _, out in client.downloaded)
        assert names == ["CHIKBR24.csv", "DENGBR24.csv"]
        client.close.assert_awaited_once()

    def test_year_filter_applied(self, files, tmp_path):
        client = _FakeClient(files)
        with patch("pysus.api.dadosgov.client.DadosGov", return_value=client):
            result = runner.invoke(
                app,
                [
                    "dadosgov",
                    "download",
                    "SINAN",
                    "-t",
                    "tok",
                    "--year",
                    "2024",
                    "-o",
                    str(tmp_path),
                ],
            )

        assert result.exit_code == 0, result.output
        assert client.download_awaits == 2

    def test_no_match_reports_no_download(self, files, tmp_path):
        client = _FakeClient(files)
        with patch("pysus.api.dadosgov.client.DadosGov", return_value=client):
            result = runner.invoke(
                app,
                [
                    "dadosgov",
                    "download",
                    "SINAN",
                    "-t",
                    "tok",
                    "--year",
                    "1999",
                    "-o",
                    str(tmp_path),
                ],
            )

        assert result.exit_code == 0, result.output
        assert "No files match" in result.output
        assert "Done." not in result.output
        assert client.download_awaits == 0
        client.close.assert_awaited_once()
