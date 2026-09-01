"""Tests for pysus.management.sync connection and helper paths."""

import asyncio
import pathlib
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from pysus.management.records import FileComparison, FileRecord
from pysus.management.sync import SyncEngine


@pytest.fixture
def engine():
    return SyncEngine(access_key="ak", secret_key="sk")


def _record(origin, name, dataset="SINAN", year=2025, **kw):
    return FileRecord(
        origin=origin,
        dataset=dataset,
        name=name,
        path=f"{origin}/{dataset}/{name}",
        year=year,
        file=kw.pop("file", MagicMock()),
        **kw,
    )


class TestPickSourceAndLabel:
    def test_pick_source_prefers_ftp(self):
        ftp = _record("ftp", "DENGBR25.dbc", file=MagicMock())
        gov = _record("dadosgov", "DENGBR25.csv.zip", file=MagicMock())
        comparison = FileComparison(key=ftp.identity_key(), records=[ftp, gov])
        assert SyncEngine._pick_source(comparison) is ftp

    def test_pick_source_falls_back_to_dadosgov(self):
        gov = _record("dadosgov", "DENGBR25.csv.zip", file=MagicMock())
        comparison = FileComparison(key=gov.identity_key(), records=[gov])
        assert SyncEngine._pick_source(comparison) is gov

    def test_pick_source_none_without_files(self):
        gov = _record("dadosgov", "DENGBR25.csv.zip", file=None)
        comparison = FileComparison(key=gov.identity_key(), records=[gov])
        assert SyncEngine._pick_source(comparison) is None

    def test_label(self):
        ftp = _record(
            "ftp",
            "PAAC2501.dbc",
            dataset="SIA",
            group="PA",
            month=1,
            state="AC",
            year=2025,
        )
        comparison = FileComparison(key=ftp.identity_key(), records=[ftp])
        assert SyncEngine._label(comparison) == "SIA/PA/2025/1/paac2501"


class TestSyncLock:
    def test_acquire_creates_lock(self, engine, tmp_path, monkeypatch):
        monkeypatch.setattr("pysus.management.sync.CACHEPATH", tmp_path)
        engine._acquire_sync_lock()
        lock = tmp_path / "ducklake" / ".sync.lock"
        assert lock.exists()
        assert int(lock.read_text()) > 0

    def test_acquire_conflict_raises(self, engine, tmp_path, monkeypatch):
        import os

        monkeypatch.setattr("pysus.management.sync.CACHEPATH", tmp_path)
        lock = tmp_path / "ducklake" / ".sync.lock"
        lock.parent.mkdir(parents=True, exist_ok=True)
        lock.write_text(str(os.getpid()))
        with pytest.raises(Exception, match="holds the catalog lock"):
            engine._acquire_sync_lock()

    def test_acquire_steals_dead_lock(self, engine, tmp_path, monkeypatch):
        monkeypatch.setattr("pysus.management.sync.CACHEPATH", tmp_path)
        lock = tmp_path / "ducklake" / ".sync.lock"
        lock.parent.mkdir(parents=True, exist_ok=True)
        lock.write_text("99999999")
        engine._acquire_sync_lock()
        assert int(lock.read_text()) != 99999999

    def test_release_removes_lock(self, engine, tmp_path, monkeypatch):
        monkeypatch.setattr("pysus.management.sync.CACHEPATH", tmp_path)
        engine._acquire_sync_lock()
        engine._release_sync_lock()
        assert not (tmp_path / "ducklake" / ".sync.lock").exists()

    def test_pid_alive(self):
        import os

        assert SyncEngine._pid_alive(os.getpid())
        assert not SyncEngine._pid_alive(99999999)


class TestDownloadOnce:
    @pytest.mark.asyncio
    async def test_download_once_ftp_pooled(self, engine, tmp_path):
        ftp = MagicMock()
        ftp.size.return_value = 5
        file = MagicMock()
        file.path = "/remote/X.dbc"
        client = MagicMock()
        client.ftp = ftp

        def _retrbinary(cmd, cb):
            cb(b"hello")

        ftp.retrbinary = _retrbinary
        out = tmp_path / "x.dbc"

        with patch(
            "anyio.to_thread.run_sync",
            new=AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a)),
        ):
            await engine._download_once(file, out, ftp_client=client)

        assert out.read_bytes() == b"hello"

    @pytest.mark.asyncio
    async def test_download_once_reconnects_broken_pool(self, engine, tmp_path):
        file = MagicMock()
        file.path = "/remote/X.dbc"
        client = MagicMock()
        broken_ftp = MagicMock()
        broken_ftp.size.side_effect = OSError("broken")
        client.ftp = None

        async def _connect():
            client.ftp = broken_ftp

        client.connect = _connect
        out = tmp_path / "x.dbc"

        with pytest.raises(OSError):
            await engine._download_once(file, out, ftp_client=client)
        assert client._ftp is None  # reset after failure

    @pytest.mark.asyncio
    async def test_download_once_falls_back_to_file(self, engine, tmp_path):
        file = MagicMock()
        file.client = MagicMock()
        file.client.ftp = None
        out = tmp_path / "x.dbc"
        file._download = AsyncMock(return_value=out)
        await engine._download_once(file, out)
        file._download.assert_awaited_once()
        kwargs = file._download.await_args.kwargs
        assert kwargs["output"] == out
        assert callable(kwargs["callback"])  # live progress → stall watch


class TestStallWatch:
    @pytest.mark.asyncio
    async def test_completes_when_progress_keeps_poking(self):
        from pysus.management.sync import _StallWatch

        watch = _StallWatch("X", timeout=0.2, poll=0.02)

        async def slow_but_alive():
            for _ in range(10):
                watch.poke()
                await asyncio.sleep(0.02)
            return "done"

        assert await watch.run(slow_but_alive()) == "done"

    @pytest.mark.asyncio
    async def test_aborts_when_progress_stalls(self):
        from pysus.management.sync import _StallWatch

        watch = _StallWatch("X", timeout=0.2, poll=0.02)
        cancelled = []

        async def stuck():
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                cancelled.append(1)
                raise

        with pytest.raises(TimeoutError):
            await watch.run(stuck())
        assert cancelled == [1]

    @pytest.mark.asyncio
    async def test_fast_finish_with_no_ticks(self):
        from pysus.management.sync import _StallWatch

        watch = _StallWatch("X", timeout=60, poll=0.02)

        async def instant():
            return "ok"

        assert await watch.run(instant()) == "ok"


class TestDownloadRawWithRetry:
    @pytest.mark.asyncio
    async def test_retries_then_succeeds(self, engine, tmp_path):
        file = MagicMock()
        file.basename = "X.dbc"
        attempts = []

        async def _once(f, output, ftp_client=None):
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionResetError("drop")
            output.write_bytes(b"data")
            return output

        with patch.object(
            engine, "_download_once", new=AsyncMock(side_effect=_once)
        ):
            with patch(
                "pysus.management.sync.asyncio.sleep",
                new=AsyncMock(),
            ):
                raw = await engine._download_raw_with_retry(file)
        assert raw.read_bytes() == b"data"
        assert len(attempts) == 3

    @pytest.mark.asyncio
    async def test_exhausts_retries(self, engine):
        file = MagicMock()
        file.basename = "X.dbc"
        with patch.object(
            engine,
            "_download_once",
            new=AsyncMock(side_effect=TimeoutError("slow")),
        ):
            with patch(
                "pysus.management.sync.asyncio.sleep",
                new=AsyncMock(),
            ):
                with pytest.raises(RuntimeError, match="Failed to download"):
                    await engine._download_raw_with_retry(file, max_retries=2)


class TestConvertAndUpload:
    @pytest.mark.asyncio
    async def test_convert_and_upload(self, engine, tmp_path):
        raw = tmp_path / "X.dbc"
        raw.write_bytes(b"raw")
        parquet = tmp_path / "X.parquet"
        parquet.write_bytes(b"pq")

        fake_parquet = MagicMock()
        fake_parquet.path = parquet
        fake_parquet.rows = 10
        fake_parquet.schema = "schema"

        local_file = MagicMock()
        local_file.to_parquet = AsyncMock(return_value=fake_parquet)

        engine.s3_key_for = MagicMock(return_value="public/data/k")
        with patch(
            "pysus.api.extensions.ExtensionFactory",
            MagicMock(instantiate=AsyncMock(return_value=local_file)),
        ):
            with patch(
                "pysus.management.sync.upload_s3", new=AsyncMock()
            ) as mock_upload:
                with patch(
                    "anyio.to_thread.run_sync",
                    new=AsyncMock(
                        side_effect=lambda fn, *a, **kw: fn(*a, **kw)
                    ),
                ):
                    payload = await engine._convert_and_upload(MagicMock(), raw)
        assert payload["s3_key"] == "public/data/k"
        assert payload["rows"] == 10
        assert payload["schema"] == "schema"
        assert len(payload["raw_digest"]) == 64
        mock_upload.assert_awaited_once()
        assert not raw.exists()
        assert not parquet.exists()

    @pytest.mark.asyncio
    async def test_convert_and_upload_non_tabular(self, engine, tmp_path):
        raw = tmp_path / "X.xyz"
        raw.write_bytes(b"raw")

        class NotTabular:
            pass

        fake = MagicMock()
        fake.basename = "X.xyz"
        engine.s3_key_for = MagicMock(return_value="public/data/k")
        with patch(
            "pysus.api.extensions.ExtensionFactory",
            MagicMock(instantiate=AsyncMock(return_value=NotTabular())),
        ):
            with pytest.raises(RuntimeError, match="cannot convert"):
                await engine._convert_and_upload(fake, raw)


class TestCatalogRows:
    def test_catalog_rows(self, engine):
        writer = MagicMock()
        writer.ensure_dataset.return_value = 8
        writer.ensure_group.return_value = 3
        writer.get_file.return_value = (99, None)

        central_cursor = MagicMock()
        dataset_cursor = MagicMock()
        columns_cursor = MagicMock()
        group = MagicMock()
        group.name = "DENG"
        file = MagicMock()
        file.dataset.name = "SINAN"
        file.dataset.long_name = "Sistema..."
        file.dataset.description = "desc"
        file.group = group
        file.path = "/ftp/x"
        file.year = 2025
        file.month = None
        file.state = None
        file.client.name = "ftp"

        payload = {
            "s3_key": "public/data/ftp/sinan/DENG/2025/_/BR/X.parquet",
            "size": 100,
            "rows": 5,
            "schema": MagicMock(),
            "raw_digest": "a" * 64,
            "parquet_digest": "b" * 64,
        }

        with patch.object(
            SyncEngine, "writer", new_callable=PropertyMock
        ) as mock_writer_prop:
            mock_writer_prop.return_value = writer
            engine._catalog_rows(
                central_cursor, dataset_cursor, columns_cursor, file, payload
            )

        writer.ensure_dataset.assert_called_once()
        writer.upsert_file.assert_called_once()
        writer.link_columns.assert_called_once()
        kwargs = writer.upsert_file.call_args.kwargs
        assert kwargs["path"] == payload["s3_key"]
        assert kwargs["sha256"] == payload["parquet_digest"]
        assert kwargs["source_sha256"] == payload["raw_digest"]
        assert kwargs["origin"] == "ftp"


class TestCatalogWriteEntry:
    def test_catalog_write_entry(self, engine):
        adapter = MagicMock()
        central = MagicMock()
        columns = MagicMock()
        writer = MagicMock()
        writer.get_file.return_value = (42, True)
        file = MagicMock()
        payload = {
            "s3_key": "k",
            "size": 10,
            "rows": 1,
            "schema": MagicMock(),
            "raw_digest": "a",
            "parquet_digest": "b",
        }

        # nested transactions: use real context managers via MagicMock
        class _Ctx:
            def __init__(self, cursor):
                self.cursor = cursor

            def __enter__(self):
                return MagicMock(), self.cursor

            def __exit__(self, *a):
                return False

        adapter.transaction = MagicMock(return_value=_Ctx(MagicMock()))
        central.transaction = MagicMock(return_value=_Ctx(MagicMock()))
        columns.transaction = MagicMock(return_value=_Ctx(MagicMock()))

        with patch.object(
            SyncEngine, "writer", new_callable=PropertyMock
        ) as mock_writer_prop:
            mock_writer_prop.return_value = writer
            engine._catalog_write_entry(
                adapter, central, columns, file, payload
            )

        adapter.transaction.assert_called_once()
        writer._ensure_management_columns.assert_called_once()


class TestDedupeS3Artifacts:
    @pytest.mark.asyncio
    async def test_keeps_newest_deletes_others(self, engine):
        from datetime import datetime

        from pysus.api.ducklake.catalog.adapters import DatasetAdapter

        older = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="public/data/ftp/sinan/DENG/2025/_/BR/DENGBR25.parquet",
            year=2025,
            source_modified=datetime(2026, 1, 1),
            size=100,
        )
        newer = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="public/data/ftp/sinan/DENG/2025/_/BR/DENGBR25.parquet",
            year=2025,
            source_modified=datetime(2026, 5, 16),
            size=200,
        )

        class _Ctx:
            def __init__(self, conn):
                self.conn = conn

            def __enter__(self):
                return self.conn, self.conn

            def __exit__(self, *a):
                return False

        conn = MagicMock()
        adapter = MagicMock(spec=DatasetAdapter)
        adapter.transaction = MagicMock(return_value=_Ctx(conn))

        ducklake = MagicMock()
        ducklake.get_dataset_adapter.return_value = adapter
        engine._ducklake = ducklake

        import boto3

        mock_s3 = MagicMock()
        with patch.object(boto3, "client", return_value=mock_s3):
            await engine._dedupe_s3_artifacts([older, newer])

        # newer artifact survives; older object deleted (same origin)
        deleted_keys = [
            c.kwargs["Key"] for c in mock_s3.delete_object.call_args_list
        ]
        assert deleted_keys == [older.path]
        cursor_calls = conn.execute.call_args_list
        assert any(
            "DELETE FROM pysus.files" in str(c.args[0]) for c in cursor_calls
        )
        assert adapter.mark_dirty.called

    @pytest.mark.asyncio
    async def test_keeps_cross_origin_mirrors(self, engine):
        """Per-origin mirroring: FTP and DadosGov artifacts of the same
        logical file are independent and must both survive dedupe."""
        from datetime import datetime

        ftp = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="public/data/ftp/sinan/DENG/2025/_/BR/DENGBR25.parquet",
            year=2025,
            source_modified=datetime(2026, 1, 1),
            size=100,
        )
        gov = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="public/data/dadosgov/sinan/DENG/2025/_/BR/"
            "DENGBR25.parquet",
            year=2025,
            source_modified=datetime(2026, 5, 16),
            size=200,
        )

        import boto3

        mock_s3 = MagicMock()
        with patch.object(boto3, "client", return_value=mock_s3):
            with patch.object(engine, "_require_ducklake") as req:
                req.return_value = MagicMock()
                await engine._dedupe_s3_artifacts([ftp, gov])

        assert not mock_s3.delete_object.called


class TestRunFtpOnlyTerminates:
    """Regression: an FTP-only run used to hang because writers_total
    was 0 (no gov workers) so the catalog writer never got a sentinel."""

    @pytest.mark.asyncio
    async def test_ftp_only_run_completes(self, engine, monkeypatch):
        from pysus.management.records import FileRecord

        ftp_rec = FileRecord(
            origin="ftp",
            dataset="SINAN",
            name="DENGBR25.dbc",
            path="/ftp/sinan/DENG/2025/DENGBR25.dbc",
            year=2025,
            group="DENG",
            size=100,
            file=MagicMock(),
        )

        records = {
            "ducklake": [],
            "ftp": [ftp_rec],
            "dadosgov": [],
            "saude": [],
        }

        mock_inv = MagicMock()
        mock_inv.collect = AsyncMock(
            side_effect=lambda origin, **kw: records.get(origin, [])
        )

        ducklake = MagicMock()
        ducklake.catalog_adapter.ensure_connected = AsyncMock()
        ducklake.catalog_adapter.connect = AsyncMock()
        ducklake.columns_adapter.ensure_connected = AsyncMock()
        ducklake.columns_adapter.connect = AsyncMock()
        ducklake.get_dataset_adapter.return_value = MagicMock(
            ensure_connected=AsyncMock(),
            transaction=MagicMock(),
            mark_dirty=MagicMock(),
        )

        class _Ctx:
            def __init__(self):
                self.conn = MagicMock()

            def __enter__(self):
                return self.conn, self.conn

            def __exit__(self, *a):
                return False

        ducklake.catalog_adapter.transaction = MagicMock(return_value=_Ctx())
        ducklake.columns_adapter.transaction = MagicMock(return_value=_Ctx())
        dataset_ctx = _Ctx()
        dataset_adapter = MagicMock(
            ensure_connected=AsyncMock(),
            transaction=MagicMock(return_value=dataset_ctx),
            mark_dirty=MagicMock(),
        )
        ducklake.get_dataset_adapter.return_value = dataset_adapter

        client = MagicMock()
        client.connect = AsyncMock()
        client.ftp = MagicMock()

        engine._ducklake = ducklake
        engine.access_key = "ak"
        engine.secret_key = "sk"

        writer = MagicMock()
        writer.ensure_dataset.return_value = 1
        writer.ensure_group.return_value = 1
        writer.get_file.return_value = (1, None)
        writer.link_columns = MagicMock()
        writer.upsert_file = MagicMock()
        writer._ensure_management_columns = MagicMock()

        async def fake_convert_and_upload(file, raw, callback=None):
            return {
                "s3_key": "k",
                "size": 1,
                "rows": 1,
                "schema": MagicMock(),
                "raw_digest": "a" * 64,
                "parquet_digest": "b" * 64,
            }

        engine._convert_and_upload = AsyncMock(
            side_effect=fake_convert_and_upload
        )
        engine._download_raw_with_retry = AsyncMock(return_value=MagicMock())

        with patch.object(engine, "_require_pysus", return_value=MagicMock()):
            with patch(
                "pysus.management.sync.Inventory", return_value=mock_inv
            ):
                with patch(
                    "pysus.api.ftp.client.FTP", return_value=client
                ) as mock_ftp_cls:
                    mock_ftp_cls.return_value.connect = AsyncMock()
                    with patch.object(engine, "_checkpoint", new=AsyncMock()):
                        with patch.object(
                            SyncEngine,
                            "writer",
                            new_callable=PropertyMock,
                        ) as mock_writer_prop:
                            mock_writer_prop.return_value = writer
                            report = await engine.run(
                                datasets=["SINAN"],
                                checkpoint_every=500,
                            )

        assert report.summary()["uploaded"] == 1


class TestCatalogedBefore:
    """``_cataloged_before`` drives the ``--reupload-before`` re-upload."""

    def _comparison(self, modified):
        s3 = _record(
            "ducklake",
            "DENGBR25.parquet",
            modified=modified,
            file=MagicMock(),
        )
        return FileComparison(key=s3.identity_key(), records=[s3])

    def test_none_cutoff_never_matches(self):
        comparison = self._comparison(modified=None)
        assert SyncEngine._cataloged_before(comparison, None) is False

    def test_old_s3_row_matches(self):
        from datetime import datetime

        comparison = self._comparison(modified=datetime(2026, 6, 1))
        cutoff = datetime(2026, 7, 6)
        assert SyncEngine._cataloged_before(comparison, cutoff) is True

    def test_new_s3_row_does_not_match(self):
        from datetime import datetime

        comparison = self._comparison(modified=datetime(2026, 8, 1))
        cutoff = datetime(2026, 7, 6)
        assert SyncEngine._cataloged_before(comparison, cutoff) is False

    def test_missing_modified_does_not_match(self):
        from datetime import datetime

        comparison = self._comparison(modified=None)
        cutoff = datetime(2026, 7, 6)
        assert SyncEngine._cataloged_before(comparison, cutoff) is False


class TestPreconnectResilient:
    """Pre-connecting a dataset catalog must not crash the whole run.

    Some datasets (e.g. saude-only ``arboviroses``) have no public
    catalog duckdb on the object storage (403). Preconnect is
    best-effort: the adapter is fetched on demand if a write needs it.
    """

    @pytest.mark.asyncio
    async def test_preconnect_skips_unavailable_catalog(self, engine):
        ducklake = MagicMock()
        ducklake.catalog_adapter.ensure_connected = AsyncMock()
        ducklake.columns_adapter.ensure_connected = AsyncMock()
        engine._ducklake = ducklake

        records = {
            "ducklake": [],
            "ftp": [_record("ftp", "DENGBR25.dbc", dataset="SINAN")],
            "dadosgov": [],
            "saude": [_record("saude", "x.jsonl", dataset="arboviroses")],
        }
        failing = MagicMock()
        failing.ensure_connected = AsyncMock(
            side_effect=RuntimeError("403 Forbidden")
        )
        good = MagicMock()
        good.ensure_connected = AsyncMock()

        engine._dataset_adapter_by_name = MagicMock(
            side_effect=lambda name: (good if name == "sinan" else failing)
        )

        await engine._preconnect_adapters(records)

        failing.ensure_connected.assert_awaited_once()
        good.ensure_connected.assert_awaited_once()


class TestResumeJournal:
    """The resume journal records completed files for pause/resume."""

    def test_journal_roundtrip(self, tmp_path):
        from pysus.management.records import (
            IdentityKey,
            SyncOutcome,
            load_journal_keys,
            write_journal_line,
        )

        key = IdentityKey(
            dataset="SINAN",
            group="DENG",
            year=2025,
            month=None,
            state=None,
            stem="dengbr25",
        )
        journal = tmp_path / "reupload-2026-07-07.jsonl"
        write_journal_line(
            journal,
            SyncOutcome(key=key, origin="ftp", status="uploaded"),
        )
        write_journal_line(
            journal,
            SyncOutcome(
                key=key,
                origin="ftp",
                status="failed",
                detail="transient",
            ),
        )
        keys = load_journal_keys(journal)
        assert keys == {key}

    def test_load_journal_excludes_failed(self, tmp_path):
        """A failed entry must be retried on the next run, not skipped."""
        from pysus.management.records import (
            IdentityKey,
            SyncOutcome,
            load_journal_keys,
            write_journal_line,
        )

        journal = tmp_path / "journal.jsonl"
        key = IdentityKey(
            dataset="SIH",
            group="RD",
            year=2024,
            month=6,
            state="SP",
            stem="rdsp2406",
        )
        write_journal_line(
            journal,
            SyncOutcome(key=key, origin="ftp", status="failed"),
        )
        keys = load_journal_keys(journal)
        assert keys == set()

    def test_load_journal_missing_file(self, tmp_path):
        from pysus.management.records import load_journal_keys

        assert load_journal_keys(tmp_path / "nope.jsonl") == set()


class TestRunResumeSkipsDoneFiles:
    """A resumed run must not re-download files already processed."""

    @pytest.mark.asyncio
    async def test_resume_skips_uploaded_key(self, engine, monkeypatch):
        from pysus.management.records import IdentityKey

        ftp_rec = _record("ftp", "DENGBR25.dbc", year=2025, group="DENG")
        records = {
            "ducklake": [],
            "ftp": [ftp_rec],
            "dadosgov": [],
            "saude": [],
        }

        mock_inv = MagicMock()
        mock_inv.collect = AsyncMock(
            side_effect=lambda origin, **kw: records.get(origin, [])
        )

        ducklake = MagicMock()
        ducklake.catalog_adapter.ensure_connected = AsyncMock()
        ducklake.catalog_adapter.connect = AsyncMock()
        ducklake.columns_adapter.ensure_connected = AsyncMock()
        ducklake.columns_adapter.connect = AsyncMock()
        engine._ducklake = ducklake
        engine.access_key = "ak"
        engine.secret_key = "sk"

        engine._convert_and_upload = AsyncMock()
        engine._download_raw_with_retry = AsyncMock()

        resume_key = ftp_rec.identity_key()
        assert resume_key == IdentityKey(
            dataset="SINAN",
            group="DENG",
            year=2025,
            month=None,
            state=None,
            stem="dengbr25",
        )

        with patch.object(engine, "_require_pysus", return_value=MagicMock()):
            with patch(
                "pysus.management.sync.Inventory", return_value=mock_inv
            ):
                with patch(
                    "pysus.api.ftp.client.FTP", return_value=MagicMock()
                ):
                    with patch.object(
                        SyncEngine,
                        "writer",
                        new_callable=PropertyMock,
                    ) as mock_writer_prop:
                        mock_writer_prop.return_value = MagicMock()
                        report = await engine.run(
                            datasets=["SINAN"],
                            resume={resume_key},
                        )

        assert report.summary()["uploaded"] == 0
        assert report.summary()["skipped"] == 1
        engine._convert_and_upload.assert_not_awaited()


class TestPerOriginMirroring:
    """Mirroring is per-origin: an FTP mirror coexists with the DadosGov
    twin of the same logical file, and each is decided independently."""

    def test_mirror_for_origin_finds_origin_specific_mirror(self):
        from pysus.management.records import FileRecord

        ftp = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="public/data/ftp/sinan/DENG/2025/_/BR/DENGBR25.parquet",
            year=2025,
            group="DENG",
            source_size=100,
        )
        gov = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="public/data/dadosgov/sinan/DENG/2025/_/BR/"
            "DENGBR25.parquet",
            year=2025,
            group="DENG",
            source_size=99,
        )
        comparison = FileComparison(key=ftp.identity_key(), records=[ftp, gov])
        assert comparison.mirror_for_origin("ftp") is ftp
        assert comparison.mirror_for_origin("dadosgov") is gov
        assert comparison.mirror_for_origin("saude") is None

    def test_mirror_for_origin_none_when_other_origin_only(self):
        from pysus.management.records import FileRecord

        gov = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="public/data/dadosgov/sinan/DENG/2025/_/BR/"
            "DENGBR25.parquet",
            year=2025,
            group="DENG",
        )
        comparison = FileComparison(key=gov.identity_key(), records=[gov])
        assert comparison.mirror_for_origin("ftp") is None

    def test_s3_origin_stale_only_compares_that_origin(self, engine):
        from pysus.management.records import FileRecord

        ftp = _record("ftp", "DENGBR25.dbc", year=2025, group="DENG", size=100)
        gov = _record(
            "dadosgov", "DENGBR25.csv.zip", year=2025, group="DENG", size=999
        )
        mirror = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="public/data/ftp/sinan/DENG/2025/_/BR/DENGBR25.parquet",
            year=2025,
            group="DENG",
            source_size=100,
        )
        comparison = FileComparison(
            key=ftp.identity_key(), records=[ftp, gov, mirror]
        )
        assert not SyncEngine._s3_origin_stale(comparison, "ftp", mirror)
        assert SyncEngine._s3_origin_stale(comparison, "dadosgov", mirror)

    @pytest.mark.asyncio
    async def test_ftp_file_uploaded_even_when_dadosgov_twin_mirrored(
        self, engine
    ):
        """Regression: an FTP file whose DadosGov twin is already on S3 in
        the DadosGov path must still be mirrored under its FTP origin path
        (previously the whole comparison was skipped via origin-blind
        ``is_on_s3``)."""
        from datetime import datetime

        from pysus.management.records import FileRecord

        ftp_rec = _record("ftp", "DENGBR25.dbc", year=2025, group="DENG")
        duck = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="public/data/dadosgov/sinan/DENG/2025/_/BR/"
            "DENGBR25.parquet",
            year=2025,
            group="DENG",
            source_size=100,
            modified=datetime(2026, 1, 2),
            file=MagicMock(),
        )
        records = {
            "ducklake": [duck],
            "ftp": [ftp_rec],
            "dadosgov": [],
            "saude": [],
        }

        mock_inv = MagicMock()
        mock_inv.collect = AsyncMock(
            side_effect=lambda origin, **kw: records.get(origin, [])
        )

        ducklake = MagicMock()
        ducklake.catalog_adapter.ensure_connected = AsyncMock()
        ducklake.catalog_adapter.connect = AsyncMock()
        ducklake.columns_adapter.ensure_connected = AsyncMock()
        ducklake.columns_adapter.connect = AsyncMock()
        engine._ducklake = ducklake
        engine.access_key = "ak"
        engine.secret_key = "sk"
        engine._convert_and_upload = AsyncMock(return_value=MagicMock())
        engine._download_raw_with_retry = AsyncMock(return_value=pathlib.Path())
        engine._catalog_write_entry = MagicMock()

        with patch.object(engine, "_require_pysus", return_value=MagicMock()):
            with patch(
                "pysus.management.sync.Inventory", return_value=mock_inv
            ):
                with patch(
                    "pysus.api.ftp.client.FTP",
                    return_value=MagicMock(connect=AsyncMock()),
                ):
                    with patch.object(
                        SyncEngine,
                        "writer",
                        new_callable=PropertyMock,
                    ) as mock_writer_prop:
                        mock_writer_prop.return_value = MagicMock()
                        report = await engine.run(datasets=["SINAN"])

        assert report.summary()["uploaded"] == 1
        assert report.summary()["skipped"] == 0

    def test_resume_origins_loads_origin_and_wildcard(self, tmp_path):
        from pysus.management.records import (
            IdentityKey,
            SyncOutcome,
            load_journal_origins,
            write_journal_line,
        )

        key = IdentityKey(
            dataset="SINAN",
            group="DENG",
            year=2025,
            month=None,
            state=None,
            stem="dengbr25",
        )
        journal = tmp_path / "j.jsonl"
        write_journal_line(
            journal, SyncOutcome(key=key, origin="ftp", status="uploaded")
        )
        legacy = tmp_path / "l.jsonl"
        legacy.write_text(
            '{"dataset": "SINAN", "group": "DENG", "year": 2024, '
            '"stem": "dengbr24", "status": "uploaded"}\n',
            encoding="utf-8",
        )
        assert load_journal_origins(journal) == {key: {"ftp"}}
        legacy_key = IdentityKey(
            dataset="SINAN",
            group="DENG",
            year=2024,
            month=None,
            state=None,
            stem="dengbr24",
        )
        assert load_journal_origins(legacy) == {legacy_key: {"*"}}

    def test_resume_covers_wildcard_and_named_origin(self):
        from pysus.management.records import IdentityKey

        key = IdentityKey(
            dataset="SINAN",
            group="DENG",
            year=2025,
            month=None,
            state=None,
            stem="dengbr25",
        )
        origins = {key: {"dadosgov"}}
        assert SyncEngine._resume_covers(origins, key, "dadosgov")
        assert not SyncEngine._resume_covers(origins, key, "ftp")
        wild = {key: {"*"}}
        assert SyncEngine._resume_covers(wild, key, "ftp")
        assert SyncEngine._resume_covers(None, key, "ftp")


class TestCheck:
    @pytest.mark.asyncio
    async def test_check_classifies_missing_outdated_current(self):
        from datetime import datetime

        engine = SyncEngine(access_key="ak", secret_key="sk")
        engine.dadosgov_token = None

        s3_rec = _record(
            "ducklake",
            "DENGBR20.parquet",
            year=2020,
            modified=datetime(2026, 1, 2),
            source_modified=datetime(2026, 1, 1),
            source_size=100,
        )
        current = _record(
            "ftp",
            "DENGBR20.dbc",
            year=2020,
            modified=datetime(2026, 1, 1),
            size=100,
        )
        # a mirrored file whose FTP origin was updated afterwards
        s3_chik = _record(
            "ducklake",
            "CHIKBR22.parquet",
            year=2022,
            modified=datetime(2026, 1, 2),
            source_modified=datetime(2026, 1, 1),
            source_size=50,
        )
        outdated = _record(
            "ftp",
            "CHIKBR22.dbc",
            year=2022,
            modified=datetime(2026, 5, 1),
            size=50,
        )
        missing = _record(
            "ftp",
            "DENGBR00.dbc",
            year=2000,
            modified=datetime(2026, 1, 1),
            size=30,
        )

        # ducklake + ftp records returned by inventory.collect
        records = {
            "ducklake": [s3_rec, s3_chik],
            "ftp": [current, outdated, missing],
        }
        mock_inv = MagicMock()
        mock_inv.collect = AsyncMock(
            side_effect=lambda origin, datasets=None, **kw: records.get(
                origin, []
            )
        )

        with patch.object(engine, "_require_pysus", return_value=MagicMock()):
            with patch(
                "pysus.management.sync.Inventory", return_value=mock_inv
            ):
                checks = await engine.check(datasets=["SINAN"])

        sinan = checks["SINAN"]
        assert len(sinan.missing) == 1
        assert len(sinan.outdated) == 1
        assert len(sinan.current) == 1
        assert sinan.needs_update is True
