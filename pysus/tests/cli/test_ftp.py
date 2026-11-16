"""Tests for pysus.cli.ftp CLI commands (Phase 2.1)."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from pysus.cli import app
from typer.testing import CliRunner

runner = CliRunner()


def _fake_file(path: str) -> MagicMock:
    f = MagicMock()
    f.path = path
    return f


class _FakeDataset:
    def __init__(self, files: list[MagicMock]) -> None:
        self._files = files

    async def search(self, **kwargs) -> list[MagicMock]:
        return self._files


class _FakeFTP:
    """Minimal async FTP client stand-in for the CLI paths."""

    def __init__(self, files: list[MagicMock]) -> None:
        self._files = files
        self.connect = AsyncMock()
        self.close = AsyncMock()
        self.downloaded: list[tuple[str, Path]] = []
        self.download_awaits = 0

        class _Named(_FakeDataset):
            def __repr__(self) -> str:
                return "<fake SINAN dataset>"

        # The CLI identifies the dataset by its class name, so the stand-in
        # has to present one.
        _Named.__name__ = "SINAN"

        self.datasets = AsyncMock(return_value=[_Named(files)])

    async def download(self, file, output) -> Path:
        self.downloaded.append((str(file.path), Path(output)))
        self.download_awaits += 1
        return Path(output)


class TestFTPList:
    def test_list_all_datasets(self):
        result = runner.invoke(app, ["ftp", "list"])
        assert result.exit_code == 0
        assert "SINAN" in result.output
        assert "SIH" in result.output
        assert "Total:" in result.output


class TestFTPSearch:
    def test_search_exact_match(self):
        result = runner.invoke(app, ["ftp", "search", "SINAN"])
        assert result.exit_code == 0
        assert "SINAN" in result.output

    def test_search_fuzzy(self):
        result = runner.invoke(app, ["ftp", "search", "hospital"])
        assert result.exit_code == 0
        assert "SIH" in result.output or "CIHA" in result.output

    def test_search_no_match(self):
        result = runner.invoke(app, ["ftp", "search", "xyzzy"])
        assert result.exit_code == 1
        assert "No" in result.output


class TestFTPShow:
    def test_show_known_dataset(self):
        result = runner.invoke(app, ["ftp", "show", "sinan"])
        assert result.exit_code == 0
        assert "Dataset:" in result.output
        assert "SINAN" in result.output

    def test_show_unknown_dataset(self):
        result = runner.invoke(app, ["ftp", "show", "FAKE"])
        assert result.exit_code == 1
        assert "not found" in result.output


class TestFTPDownload:
    def test_download_bad_slug(self):
        result = runner.invoke(app, ["ftp", "download", "FAKE"])
        assert result.exit_code == 1
        assert "not found" in result.output


class TestFTPFilesCommand:
    """``files`` and ``download`` must actually drive the async client."""

    def _files(self) -> list[MagicMock]:
        return [
            _fake_file("/dissemin/publicos/SINAN/DISEASES/DENGBR25.dbc"),
            _fake_file("/dissemin/publicos/SINAN/DISEASES/DENGBR24.dbc"),
            _fake_file("/dissemin/publicos/SINAN/DISEASES/CHIKBR25.dbc"),
        ]

    def test_files_lists_remote_files(self):
        ftp = _FakeFTP(self._files())
        with patch("pysus.cli.ftp._get_ftp", return_value=ftp):
            result = runner.invoke(app, ["ftp", "files", "SINAN"])

        assert result.exit_code == 0, result.output
        assert "Files for SINAN: 3" in result.output
        assert "DENGBR25.dbc" in result.output
        ftp.connect.assert_awaited_once()
        ftp.close.assert_awaited_once()

    def test_files_applies_year_filter(self):
        ftp = _FakeFTP(self._files())
        with patch("pysus.cli.ftp._get_ftp", return_value=ftp):
            result = runner.invoke(
                app, ["ftp", "files", "SINAN", "--year", "24"]
            )

        assert result.exit_code == 0, result.output
        assert "Files for SINAN: 1" in result.output
        assert "DENGBR24.dbc" in result.output

    def test_files_no_match_exits_zero(self):
        ftp = _FakeFTP(self._files())
        with patch("pysus.cli.ftp._get_ftp", return_value=ftp):
            result = runner.invoke(
                app, ["ftp", "files", "SINAN", "--year", "1999"]
            )

        assert result.exit_code == 0, result.output
        assert "No files match" in result.output
        ftp.close.assert_awaited_once()

    def test_files_closes_ftp_on_error(self):
        ftp = _FakeFTP(self._files())
        ftp.datasets.side_effect = RuntimeError("network down")
        with patch("pysus.cli.ftp._get_ftp", return_value=ftp):
            result = runner.invoke(app, ["ftp", "files", "SINAN"])

        assert result.exit_code != 0
        ftp.close.assert_awaited_once()

    def test_download_awaits_every_transfer(self, tmp_path):
        ftp = _FakeFTP(self._files())
        with patch("pysus.cli.ftp._get_ftp", return_value=ftp):
            result = runner.invoke(
                app,
                ["ftp", "download", "SINAN", "-o", str(tmp_path)],
            )

        assert result.exit_code == 0, result.output
        assert "Downloading 3 file(s)" in result.output
        assert "Done." in result.output
        # Every file must have been awaited, not merely scheduled.
        assert len(ftp.downloaded) == 3
        assert ftp.download_awaits == 3
        assert all(
            out.parent == tmp_path for _, out in ftp.downloaded
        ), ftp.downloaded

    def test_download_writes_basenames_not_full_paths(self, tmp_path):
        ftp = _FakeFTP(self._files())
        with patch("pysus.cli.ftp._get_ftp", return_value=ftp):
            result = runner.invoke(
                app,
                ["ftp", "download", "SINAN", "-o", str(tmp_path)],
            )

        assert result.exit_code == 0, result.output
        names = sorted(out.name for _, out in ftp.downloaded)
        assert names == ["CHIKBR25.dbc", "DENGBR24.dbc", "DENGBR25.dbc"]

    def test_download_no_match_does_not_report_success(self, tmp_path):
        ftp = _FakeFTP(self._files())
        with patch("pysus.cli.ftp._get_ftp", return_value=ftp):
            result = runner.invoke(
                app,
                [
                    "ftp",
                    "download",
                    "SINAN",
                    "--year",
                    "1999",
                    "-o",
                    str(tmp_path),
                ],
            )

        assert result.exit_code == 0, result.output
        assert "No files match" in result.output
        assert "Done." not in result.output
        assert ftp.download_awaits == 0
