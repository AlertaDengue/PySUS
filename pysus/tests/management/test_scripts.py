"""Tests for pysus.management.scripts (mocked engines)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestSyncClientsScript:
    def test_load_env(self, tmp_path):
        from pysus.management.scripts.sync_clients import load_env

        env_file = tmp_path / ".env"
        env_file.write_text(
            "ACCESS_KEY=ak\nSECRET_KEY=sk\nDADOSGOV_TOKEN=tok\n"
        )
        env = load_env(str(env_file))
        assert env == {
            "ACCESS_KEY": "ak",
            "SECRET_KEY": "sk",
            "DADOSGOV_TOKEN": "tok",
        }

    def test_load_env_skips_blanks_and_comments(self, tmp_path):
        from pysus.management.scripts.sync_clients import load_env

        env_file = tmp_path / ".env"
        env_file.write_text(
            "# comment\n\n  \nACCESS_KEY=ak\nNO_EQUALS\nSECRET_KEY=sk\n"
        )
        env = load_env(str(env_file))
        assert env == {"ACCESS_KEY": "ak", "SECRET_KEY": "sk"}

    def test_load_env_strips_quotes(self, tmp_path):
        from pysus.management.scripts.sync_clients import load_env

        env_file = tmp_path / ".env"
        env_file.write_text("ACCESS_KEY=\"ak\"\nSECRET_KEY='sk'\n")
        env = load_env(str(env_file))
        assert env == {"ACCESS_KEY": "ak", "SECRET_KEY": "sk"}

    @pytest.mark.asyncio
    async def test_run(self, tmp_path, capsys):
        from pysus.management.scripts.sync_clients import run

        with patch(
            "pysus.management.scripts.sync_clients.load_env",
            return_value={
                "ACCESS_KEY": "ak",
                "SECRET_KEY": "sk",
                "DADOSGOV_TOKEN": "tok",
            },
        ):
            with patch(
                "pysus.management.scripts.sync_clients.SyncEngine"
            ) as mock_cls:
                engine = mock_cls.return_value
                engine.__aenter__ = AsyncMock(return_value=engine)
                engine.__aexit__ = AsyncMock(return_value=None)
                engine.run = AsyncMock(
                    return_value=MagicMock(summary=lambda: {"total": 1})
                )
                summary = await run(["SINAN"], 500, False, 4, 2, False)
        assert summary == {"total": 1}

    @pytest.mark.asyncio
    async def test_run_saude_only(self, tmp_path):
        from pysus.management.scripts.sync_clients import run

        with patch(
            "pysus.management.scripts.sync_clients.load_env",
            return_value={},
        ):
            with patch(
                "pysus.management.scripts.sync_clients.SyncEngine"
            ) as mock_cls:
                engine = mock_cls.return_value
                engine.__aenter__ = AsyncMock(return_value=engine)
                engine.__aexit__ = AsyncMock(return_value=None)
                engine.run = AsyncMock(
                    return_value=MagicMock(summary=lambda: {"total": 0})
                )
                await run(None, 500, False, 1, 1, saude_only=True)
                _, kwargs = engine.run.call_args
                assert kwargs["origins"] == ("ducklake", "saude")

    @pytest.mark.asyncio
    async def test_on_outcome_callback(self, tmp_path, capsys):
        from pysus.management.records import IdentityKey, SyncOutcome
        from pysus.management.scripts.sync_clients import run

        key = IdentityKey(
            dataset="X",
            group=None,
            year=2025,
            month=None,
            state=None,
            stem="x",
        )
        outcomes = [
            SyncOutcome(key=key, origin="ftp", status="uploaded"),
            SyncOutcome(key=key, origin="ftp", status="failed", detail="err"),
            SyncOutcome(
                key=key,
                origin="dadosgov",
                status="needs_token",
                detail="no token",
            ),
            SyncOutcome(key=key, origin="ftp", status="skipped"),
        ]

        with patch(
            "pysus.management.scripts.sync_clients.load_env",
            return_value={},
        ):
            with patch(
                "pysus.management.scripts.sync_clients.SyncEngine"
            ) as mock_cls:
                engine = mock_cls.return_value
                engine.__aenter__ = AsyncMock(return_value=engine)
                engine.__aexit__ = AsyncMock(return_value=None)

                captured_outcomes = []

                async def fake_run(**kwargs):
                    cb = kwargs.get("on_outcome")
                    for o in outcomes:
                        cb(o)
                    captured_outcomes.extend(outcomes)
                    return MagicMock(summary=lambda: {"total": len(outcomes)})

                engine.run = fake_run
                summary = await run(None, 500, False, 1, 1, saude_only=False)

        assert summary["total"] == 4

    def test_main_runs(self, tmp_path, capsys):
        from pysus.management.scripts import sync_clients

        with patch.object(
            sync_clients, "run", new=AsyncMock(return_value={"uploaded": 1})
        ):
            with patch.object(sync_clients, "load_env", return_value={}):
                with patch(
                    "sys.argv",
                    ["sync_clients", "--datasets", "SINAN"],
                ):
                    assert sync_clients.main() == 0


class TestCompareClientsScript:
    def test_load_env(self, tmp_path):
        from pysus.management.scripts.compare_clients import load_env

        env_file = tmp_path / ".env"
        env_file.write_text("ACCESS_KEY=ak\n")
        assert load_env(str(env_file)) == {"ACCESS_KEY": "ak"}

    @pytest.mark.asyncio
    async def test_run(self):
        from pysus.management.scripts.compare_clients import run

        engine = MagicMock()
        engine.__aenter__ = AsyncMock(return_value=engine)
        engine.__aexit__ = AsyncMock(return_value=None)
        engine.inventory.collect = AsyncMock(return_value=[])
        with patch(
            "pysus.management.scripts.compare_clients.load_env",
            return_value={"ACCESS_KEY": "ak"},
        ):
            with patch(
                "pysus.management.scripts.compare_clients.SyncEngine",
                return_value=engine,
            ):
                result = await run(["SINAN"])
        assert result["origin_counts"] == {
            "ducklake": 0,
            "ftp": 0,
            "dadosgov": 0,
        }
        assert result["reports"] == []

    def test_print_table(self, capsys):
        from pysus.management.scripts.compare_clients import print_table

        result = {
            "origin_counts": {"ftp": 1, "dadosgov": 0, "ducklake": 0},
            "reports": [
                {
                    "dataset": "SINAN",
                    "total": 1,
                    "on_all_three": 0,
                    "on_ftp_dadosgov": 0,
                    "on_ftp_s3": 0,
                    "on_dadosgov_s3": 0,
                    "ftp_only": 1,
                    "dadosgov_only": 0,
                    "s3_only": 0,
                    "examples": {"ftp_only": ["SINAN/DENG/2025/-/dengbr25"]},
                }
            ],
        }
        print_table(result)
        out = capsys.readouterr().out
        assert "SINAN" in out
        assert "ftp_only" in out

    def test_main_json(self, tmp_path, capsys):
        from pysus.management.scripts import compare_clients

        with patch.object(
            compare_clients,
            "run",
            new=AsyncMock(
                return_value={
                    "origin_counts": {},
                    "reports": [],
                }
            ),
        ):
            with patch(
                "sys.argv",
                ["compare_clients", "--json", "--datasets", "SINAN"],
            ):
                assert compare_clients.main() == 0
        assert '"origin_counts"' in capsys.readouterr().out


class TestRelayoutBucketScript:
    def test_load_env(self, tmp_path):
        from pysus.management.scripts.relayout_bucket import load_env

        env_file = tmp_path / ".env"
        env_file.write_text("ACCESS_KEY=ak\nSECRET_KEY=sk\n")
        assert load_env(str(env_file)) == {
            "ACCESS_KEY": "ak",
            "SECRET_KEY": "sk",
        }

    def test_main_dry_run(self, tmp_path, capsys):
        from pysus.management.scripts import relayout_bucket

        normalizer = MagicMock()
        normalizer._list_objects.return_value = []
        normalizer.survey_relayout.return_value = MagicMock(
            object_renames=[],
            object_deletes=[],
            catalog_fixes=[],
            catalog_row_deletes=[],
            broken_rows=[],
            raw_objects=[],
            summary=lambda: {},
        )
        normalizer.relocate_uncataloged.return_value = MagicMock(
            object_renames=[],
            object_deletes=[],
            catalog_fixes=[],
            catalog_row_deletes=[],
            broken_rows=[],
            raw_objects=[],
            summary=lambda: {},
        )
        with patch(
            "pysus.management.scripts.relayout_bucket.load_env",
            return_value={"ACCESS_KEY": "ak", "SECRET_KEY": "sk"},
        ):
            with patch(
                "pysus.management.scripts.relayout_bucket.BucketNormalizer",
                return_value=normalizer,
            ):
                with patch(
                    "pysus.management.scripts.relayout_bucket.httpx.Client"
                ) as mock_http:
                    response = MagicMock()
                    response.content = b""
                    response.raise_for_status = MagicMock()
                    http_client = mock_http.return_value.__enter__
                    http_get = http_client.return_value.get
                    http_get.return_value = response
                    with patch(
                        "sys.argv",
                        ["relayout_bucket", "--dry-run"],
                    ):
                        assert relayout_bucket.main() == 0
        assert "DRY RUN" in capsys.readouterr().out
