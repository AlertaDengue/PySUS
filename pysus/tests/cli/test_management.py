"""Tests for pysus.cli.management (dev-only S3 sync commands)."""

from unittest.mock import AsyncMock, MagicMock, patch

from pysus.cli import app
from typer.testing import CliRunner

runner = CliRunner()


def _make_engine(summary: dict):
    engine = MagicMock()
    engine.__aenter__ = AsyncMock(return_value=engine)
    engine.__aexit__ = AsyncMock(return_value=None)
    engine.run = AsyncMock(return_value=MagicMock(summary=lambda: summary))
    engine.check = AsyncMock(return_value={})
    return engine


class TestManagementCheck:
    @patch("pysus.cli.management._load_env")
    def test_check_all_databases(self, mock_env):
        mock_env.return_value = {"ACCESS_KEY": "ak", "SECRET_KEY": "sk"}
        engine = _make_engine(
            {
                "total": 3,
                "needs_update": 1,
                "uploaded": 0,
                "skipped": 2,
                "failed": 0,
                "needs_token": 0,
            }
        )
        with patch("pysus.management.sync.SyncEngine", return_value=engine):
            result = runner.invoke(app, ["management", "check"])
        assert result.exit_code == 0
        assert "DATABASE" in result.output
        assert "TOTAL" in result.output
        engine.check.assert_awaited_once_with(datasets=None)

    @patch("pysus.cli.management._load_env")
    def test_check_specific_databases(self, mock_env):
        mock_env.return_value = {"ACCESS_KEY": "ak", "SECRET_KEY": "sk"}
        engine = _make_engine(
            {
                "total": 0,
                "needs_update": 0,
                "uploaded": 0,
                "skipped": 0,
                "failed": 0,
                "needs_token": 0,
            }
        )
        with patch("pysus.management.sync.SyncEngine", return_value=engine):
            result = runner.invoke(
                app, ["management", "check", "SINAN", "SINASC", "SIM"]
            )
        assert result.exit_code == 0
        engine.check.assert_awaited_once_with(
            datasets=["SINAN", "SINASC", "SIM"]
        )

    @patch("pysus.cli.management._load_env")
    def test_check_apply_disables_dry_run(self, mock_env):
        mock_env.return_value = {"ACCESS_KEY": "ak", "SECRET_KEY": "sk"}
        engine = _make_engine(
            {
                "total": 2,
                "needs_update": 0,
                "uploaded": 2,
                "skipped": 0,
                "failed": 0,
                "needs_token": 0,
            }
        )
        with patch("pysus.management.sync.SyncEngine", return_value=engine):
            result = runner.invoke(
                app, ["management", "check", "SINAN", "--apply"]
            )
        assert result.exit_code == 0
        _, kwargs = engine.run.call_args
        assert kwargs["dry_run"] is False
        assert kwargs["checkpoint_every"] == 500

    @patch("pysus.cli.management._load_env")
    def test_check_json_streams_freshness(self, mock_env):
        mock_env.return_value = {"ACCESS_KEY": "ak", "SECRET_KEY": "sk"}
        from pysus.management.records import DatabaseCheck

        chk = DatabaseCheck(dataset="SINAN")
        chk.add("missing", "SINAN/DENG/-/DENGBR20")
        engine = _make_engine({})
        engine.check = AsyncMock(return_value={"SINAN": chk})

        with patch("pysus.management.sync.SyncEngine", return_value=engine):
            result = runner.invoke(app, ["management", "check", "--json"])
        assert result.exit_code == 0
        assert '"dataset": "SINAN"' in result.output
        assert '"missing": 1' in result.output
        assert '"needs_update": true' in result.output

    @patch("pysus.cli.management._load_env")
    def test_check_apply_exit_code_on_failure(self, mock_env):
        mock_env.return_value = {"ACCESS_KEY": "ak", "SECRET_KEY": "sk"}
        engine = _make_engine(
            {
                "total": 2,
                "needs_update": 0,
                "uploaded": 0,
                "skipped": 0,
                "failed": 2,
                "needs_token": 0,
            }
        )
        with patch("pysus.management.sync.SyncEngine", return_value=engine):
            result = runner.invoke(
                app, ["management", "check", "SINAN", "--apply"]
            )
        assert result.exit_code == 1

    @patch("pysus.cli.management._load_env")
    def test_check_reupload_before(self, mock_env):
        mock_env.return_value = {"ACCESS_KEY": "ak", "SECRET_KEY": "sk"}
        engine = _make_engine({})
        with patch("pysus.management.sync.SyncEngine", return_value=engine):
            result = runner.invoke(
                app,
                [
                    "management",
                    "check",
                    "SINAN",
                    "--reupload-before",
                    "2026-07-06",
                ],
            )
        assert result.exit_code == 0
        engine.check.assert_awaited_once_with(datasets=["SINAN"])

    @patch("pysus.cli.management._load_env")
    def test_check_reupload_before_with_apply(self, mock_env):
        from datetime import datetime

        mock_env.return_value = {"ACCESS_KEY": "ak", "SECRET_KEY": "sk"}
        engine = _make_engine(
            {
                "total": 2,
                "needs_update": 0,
                "uploaded": 2,
                "skipped": 0,
                "failed": 0,
                "needs_token": 0,
            }
        )
        with patch("pysus.management.sync.SyncEngine", return_value=engine):
            result = runner.invoke(
                app,
                [
                    "management",
                    "check",
                    "--apply",
                    "--reupload-before",
                    "2026-07-06",
                ],
            )
        assert result.exit_code == 0
        _, kwargs = engine.run.call_args
        assert kwargs["reupload_before"] == datetime(2026, 7, 6)
        assert kwargs["dry_run"] is False

    @patch("pysus.cli.management._load_env")
    def test_check_resume_passes_journal(self, mock_env, tmp_path):
        from pysus.management.records import (
            IdentityKey,
            SyncOutcome,
            write_journal_line,
        )

        journal = tmp_path / "reupload-2026-07-06.jsonl"
        write_journal_line(
            journal,
            SyncOutcome(
                key=IdentityKey(
                    dataset="SINAN",
                    group="DENG",
                    year=2025,
                    month=None,
                    state=None,
                    stem="dengbr25",
                ),
                origin="ftp",
                status="uploaded",
            ),
        )
        mock_env.return_value = {"ACCESS_KEY": "ak", "SECRET_KEY": "sk"}
        engine = _make_engine(
            {
                "total": 1,
                "needs_update": 0,
                "uploaded": 0,
                "skipped": 1,
                "failed": 0,
                "needs_token": 0,
            }
        )
        with patch("pysus.management.sync.SyncEngine", return_value=engine):
            result = runner.invoke(
                app,
                [
                    "management",
                    "check",
                    "--apply",
                    "--reupload-before",
                    "2026-07-06",
                    "--resume",
                    str(journal),
                ],
            )
        assert result.exit_code == 0
        _, kwargs = engine.run.call_args
        assert kwargs["dry_run"] is False
        assert kwargs["journal"] == journal
        assert kwargs["resume"] == {
            IdentityKey(
                dataset="SINAN",
                group="DENG",
                year=2025,
                month=None,
                state=None,
                stem="dengbr25",
            )
        }

    @patch("pysus.cli.management._load_env")
    def test_check_resume_defaults_to_cache_dir(self, mock_env, tmp_path):
        import pysus.cli.management as management

        mock_env.return_value = {"ACCESS_KEY": "ak", "SECRET_KEY": "sk"}
        engine = _make_engine(
            {
                "total": 0,
                "needs_update": 0,
                "uploaded": 0,
                "skipped": 0,
                "failed": 0,
                "needs_token": 0,
            }
        )
        with patch.object(management, "CACHEPATH", tmp_path / "cache"):
            with patch("pysus.management.sync.SyncEngine", return_value=engine):
                result = runner.invoke(
                    app,
                    [
                        "management",
                        "check",
                        "--apply",
                        "--reupload-before",
                        "2026-07-06",
                    ],
                )
        assert result.exit_code == 0
        _, kwargs = engine.run.call_args
        assert kwargs["journal"] == (
            tmp_path
            / "cache"
            / "management"
            / "journal"
            / "reupload-2026-07-06.jsonl"
        )
