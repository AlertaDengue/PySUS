"""Additional coverage for pysus.management.sync.

Fill gaps left by test_sync.py / test_sync_internal.py: weight gate,
connection helpers, freshness helpers, the single-file upload paths,
reprocess/process-comparison decisions, download gates and the
catalog/checkpoint failure branches.
"""

import asyncio
import os
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from pysus.management.catalog import sha256_of
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


def _remote_file(**kw):
    f = MagicMock()
    f.size = kw.pop("size", 100)
    f.modify = kw.pop("modify", datetime(2026, 1, 1))
    f.basename = kw.pop("basename", "X.dbc")
    f.path = kw.pop("path", "/ftp/sinan/X.dbc")
    f.year = kw.pop("year", None)
    f.month = kw.pop("month", None)
    f.state = kw.pop("state", None)
    f.dataset.name = kw.pop("dataset_name", "SINAN")
    f.dataset.long_name = "Sistema de Informação"
    f.dataset.description = "desc"
    f.group.name = kw.pop("group_name", "DENG")
    f.client.name = kw.pop("client_name", "ftp")
    client_ftp = kw.pop("client_ftp", None)
    if client_ftp is not None:
        f.client.ftp = client_ftp
    return f


def _adapter(close_raises=False):
    a = MagicMock()
    conn = MagicMock()
    if close_raises:
        conn.close.side_effect = OSError("already closed")
    a.connect = AsyncMock()
    a.raw_connection = MagicMock(return_value=conn)
    a.mark_dirty = MagicMock()
    return a


class TestWeightGate:
    def test_weight_of_caps_at_budget(self):
        from pysus.management.sync import _WeightGate

        budget = 100
        assert _WeightGate.weight_of(1000, budget) == budget
        assert _WeightGate.weight_of(budget + 1, budget) == budget

    def test_weight_of_small_files_floor(self):
        from pysus.management.sync import _SMALL_FILE_BYTES, _WeightGate

        assert _WeightGate.weight_of(1) == _SMALL_FILE_BYTES
        assert _WeightGate.weight_of(-5) == _SMALL_FILE_BYTES

    def test_weight_of_non_numeric(self):
        from pysus.management.sync import _SMALL_FILE_BYTES, _WeightGate

        assert _WeightGate.weight_of(None) == _SMALL_FILE_BYTES
        assert _WeightGate.weight_of("nope") == _SMALL_FILE_BYTES

    @pytest.mark.asyncio
    async def test_acquire_release_adjust(self):
        from pysus.management.sync import _WeightGate

        gate = _WeightGate(budget=10)
        w = await gate.acquire(4)
        assert w == 4
        new_w = await gate.adjust(4, 6)
        assert new_w == 6
        assert await gate.adjust(6, 6) == 6
        await gate.release(6)

    @pytest.mark.asyncio
    async def test_acquire_waits_for_release(self):
        from pysus.management.sync import _WeightGate

        gate = _WeightGate(budget=10)
        await gate.acquire(10)
        results = []

        async def blocked():
            w = await gate.acquire(10)
            results.append(w)

        task = asyncio.create_task(blocked())
        await asyncio.sleep(0)
        assert results == []
        await gate.release(10)
        await task
        assert results == [10]


class TestConnectionHelpers:
    def test_require_pysus_raises(self):
        engine = SyncEngine()
        with pytest.raises(Exception, match="not connected"):
            engine._require_pysus()

    def test_require_ducklake_raises(self, engine):
        with pytest.raises(Exception, match="not connected"):
            engine._require_ducklake()

    def test_writer_property_raises(self, engine):
        with pytest.raises(Exception, match="not connected"):
            engine.writer

    @pytest.mark.asyncio
    async def test_enter_creates_pysus_and_logs_in(self, engine, tmp_path):
        fake_pysus = MagicMock()
        fake_pysus.__aenter__ = AsyncMock()
        fake_pysus.get_ducklake = AsyncMock()
        ducklake = MagicMock()
        ducklake.login = AsyncMock()
        fake_pysus.get_ducklake.return_value = ducklake

        engine.pysus = fake_pysus
        engine.access_key = "ak"
        engine.secret_key = "sk"

        with patch.object(engine, "_acquire_sync_lock") as lock_mock:
            result = await engine.__aenter__(lock=False)
        assert result is engine
        fake_pysus.__aenter__.assert_awaited_once()
        ducklake.login.assert_awaited_once_with(
            access_key="ak", secret_key="sk"
        )
        lock_mock.assert_not_called()
        assert engine._ducklake is ducklake


class TestAexitAndLock:
    @pytest.mark.asyncio
    async def test_aexit_closes_catalog_and_pysus(self, engine):
        ducklake = MagicMock()
        ducklake.close = AsyncMock()
        fake_pysus = MagicMock()
        fake_pysus.__aexit__ = AsyncMock()
        engine._ducklake = ducklake
        engine.pysus = fake_pysus
        engine._changed_catalog = True

        with patch.object(engine, "_release_sync_lock") as release:
            await engine.__aexit__(None, None, None)

        ducklake.close.assert_awaited_once_with(update_catalog=True)
        release.assert_called_once()
        fake_pysus.__aexit__.assert_awaited_once()


class TestS3KeyFor:
    def test_s3_key_for_builds_hierarchical_key(self, engine):
        f = _remote_file()
        assert engine.s3_key_for(f).startswith("public/data/ftp/sinan/DENG/_")

    def test_s3_key_for_without_group(self, engine):
        f = _remote_file(group_name=None)
        key = engine.s3_key_for(f)
        assert "/_/" in key


class TestFreshnessHelpers:
    def test_is_current_false_when_modified_missing(self):
        f = _remote_file()
        assert SyncEngine._is_current(f, None) is False

    def test_is_current_true_when_file_not_newer(self):
        f = _remote_file(modify=datetime(2026, 1, 1))
        assert SyncEngine._is_current(f, datetime(2026, 2, 1)) is True

    def test_is_current_false_when_modify_raises(self):
        class _Broken:
            @property
            def modify(self):
                raise ValueError("bad")

        assert SyncEngine._is_current(_Broken(), datetime(2026, 2, 1)) is False

    def test_safe_modify_returns_none_when_raises(self):
        class _Broken:
            @property
            def modify(self):
                raise ValueError("bad")

        assert SyncEngine._safe_modify(_Broken()) is None

    def test_safe_size_zero_when_unavailable(self):
        class _NoSize:
            @property
            def size(self):
                raise AttributeError("nope")

        class _Raises:
            @property
            def size(self):
                raise ValueError("bad")

        assert SyncEngine._safe_size(_NoSize()) == 0
        assert SyncEngine._safe_size(_Raises()) == 0
        assert SyncEngine._safe_size(MagicMock(size=42)) == 42


class TestDownloadOnceGates:
    @pytest.mark.asyncio
    async def test_rejects_oversized_file(self, engine, tmp_path):
        from pysus.management.sync import _MAX_FILE_SIZE

        f = _remote_file(size=_MAX_FILE_SIZE + 1)
        with pytest.raises(RuntimeError, match="file too large"):
            await engine._download_once(f, tmp_path / "x.dbc")

    @pytest.mark.asyncio
    async def test_direct_ftp_retr(self, engine, tmp_path):
        ftp = MagicMock()

        def _retrbinary(cmd, cb):
            cb(b"hello")

        ftp.retrbinary = _retrbinary
        f = _remote_file(client_ftp=ftp)
        out = tmp_path / "x.dbc"

        with patch(
            "anyio.to_thread.run_sync",
            new=AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a)),
        ):
            result = await engine._download_once(f, out)

        assert out.read_bytes() == b"hello"
        assert result == out


class TestStaleHelpers:
    def _comparison(self, s3_record, records):
        key = s3_record.identity_key()
        return FileComparison(key=key, records=[s3_record, *records])

    def test_s3_origin_stale_false_without_mirror(self):
        ftp = _record("ftp", "DENGBR25.dbc", size=100, file=MagicMock())
        c = FileComparison(key=ftp.identity_key(), records=[ftp])
        assert SyncEngine._s3_origin_stale(c, "ftp", None) is False

    def test_s3_origin_stale_false_without_size(self):
        ftp = _record("ftp", "DENGBR25.dbc", size=100, file=MagicMock())
        mirror = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="public/data/ftp/sinan/DENG/2025/_/BR/DENGBR25.parquet",
            year=2025,
            source_size=None,
        )
        c = FileComparison(key=ftp.identity_key(), records=[ftp, mirror])
        assert SyncEngine._s3_origin_stale(c, "ftp", mirror) is False

    def test_s3_origin_stale_true_on_size_mismatch(self):
        ftp = _record("ftp", "DENGBR25.dbc", size=200, file=MagicMock())
        mirror = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="public/data/ftp/sinan/DENG/2025/_/BR/DENGBR25.parquet",
            year=2025,
            source_size=100,
        )
        c = FileComparison(key=ftp.identity_key(), records=[ftp, mirror])
        assert SyncEngine._s3_origin_stale(c, "ftp", mirror) is True

    def test_s3_is_stale_false_without_s3_size(self):
        ftp = _record("ftp", "DENGBR25.dbc", size=100, file=MagicMock())
        s3 = _record("ducklake", "DENGBR25.parquet", file=MagicMock())
        c = FileComparison(key=ftp.identity_key(), records=[s3, ftp])
        assert SyncEngine._s3_is_stale(c) is False

    def test_s3_is_stale_true_on_size_mismatch(self):
        ftp = _record("ftp", "DENGBR25.dbc", size=200, file=MagicMock())
        s3 = _record(
            "ducklake", "DENGBR25.parquet", source_size=100, file=MagicMock()
        )
        c = FileComparison(key=ftp.identity_key(), records=[s3, ftp])
        assert SyncEngine._s3_is_stale(c) is True


class TestMirrorCatalogedBefore:
    def test_none_or_no_modified(self):
        assert (
            SyncEngine._mirror_cataloged_before(None, datetime(2026, 7, 1))
            is False
        )
        mirror = _record("ducklake", "DENGBR25.parquet", file=MagicMock())
        assert (
            SyncEngine._mirror_cataloged_before(mirror, datetime(2026, 7, 1))
            is False
        )

    def test_old_mirror_matches(self):
        mirror = _record(
            "ducklake",
            "DENGBR25.parquet",
            modified=datetime(2026, 1, 1),
            file=MagicMock(),
        )
        assert SyncEngine._mirror_cataloged_before(mirror, datetime(2026, 7, 1))


class TestReprocess:
    @pytest.mark.asyncio
    async def test_dadosgov_only_without_token(self, engine):
        gov = _record("dadosgov", "X.csv.zip", file=MagicMock())
        engine.dadosgov_token = None
        c = FileComparison(key=gov.identity_key(), records=[gov])
        outcome = await engine._reprocess(c, None, False, "SINAN/X")
        assert outcome.status == "needs_token"

    @pytest.mark.asyncio
    async def test_dadosgov_with_token_uploads(self, engine):
        gov = _record("dadosgov", "X.csv.zip", file=MagicMock())
        engine.dadosgov_token = "tok"
        engine.upload_file = AsyncMock(return_value=True)
        c = FileComparison(key=gov.identity_key(), records=[gov])
        outcome = await engine._reprocess(c, None, False, "SINAN/X")
        assert outcome.status == "uploaded"
        engine.upload_file.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ftp_already_current(self, engine):
        ftp = _record("ftp", "X.dbc", file=MagicMock())
        engine.upload_file = AsyncMock(return_value=False)
        c = FileComparison(key=ftp.identity_key(), records=[ftp])
        outcome = await engine._reprocess(c, None, False, "SINAN/X")
        assert outcome.status == "skipped"

    @pytest.mark.asyncio
    async def test_authentication_error_becomes_needs_token(self, engine):
        from pysus.api.errors import AuthenticationError

        ftp = _record("ftp", "X.dbc", file=MagicMock())
        engine.upload_file = AsyncMock(
            side_effect=AuthenticationError("no token")
        )
        c = FileComparison(key=ftp.identity_key(), records=[ftp])
        outcome = await engine._reprocess(c, None, False, "SINAN/X")
        assert outcome.status == "needs_token"

    @pytest.mark.asyncio
    async def test_other_error_becomes_failed(self, engine):
        ftp = _record("ftp", "X.dbc", file=MagicMock())
        engine.upload_file = AsyncMock(side_effect=RuntimeError("boom"))
        c = FileComparison(key=ftp.identity_key(), records=[ftp])
        outcome = await engine._reprocess(c, None, False, "SINAN/X")
        assert outcome.status == "failed"

    @pytest.mark.asyncio
    async def test_no_downloadable_artifact(self, engine):
        ftp = _record("ftp", "X.dbc", file=None)
        c = FileComparison(key=ftp.identity_key(), records=[ftp])
        outcome = await engine._reprocess(c, None, False, "SINAN/X")
        assert outcome.status == "failed"
        assert "no downloadable" in outcome.detail


class TestProcessComparison:
    @pytest.mark.asyncio
    async def test_on_s3_not_stale_skips(self, engine):
        ftp = _record("ftp", "DENGBR25.dbc", size=100, file=MagicMock())
        s3 = _record(
            "ducklake", "DENGBR25.parquet", source_size=100, file=MagicMock()
        )
        c = FileComparison(key=ftp.identity_key(), records=[s3, ftp])
        outcome = await engine._process_comparison(c)
        assert outcome.status == "skipped"
        assert outcome.origin == "ducklake"

    @pytest.mark.asyncio
    async def test_on_s3_forced_reprocesses(self, engine):
        ftp = _record("ftp", "DENGBR25.dbc", size=100, file=MagicMock())
        s3 = _record(
            "ducklake", "DENGBR25.parquet", source_size=100, file=MagicMock()
        )
        c = FileComparison(key=ftp.identity_key(), records=[s3, ftp])
        engine._reprocess = AsyncMock(return_value=MagicMock(status="uploaded"))
        outcome = await engine._process_comparison(c, force=True)
        assert outcome.status == "uploaded"
        engine._reprocess.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_not_on_s3_reprocesses(self, engine):
        ftp = _record("ftp", "DENGBR25.dbc", size=100, file=MagicMock())
        c = FileComparison(key=ftp.identity_key(), records=[ftp])
        engine._reprocess = AsyncMock(return_value=MagicMock(status="uploaded"))
        outcome = await engine._process_comparison(c)
        assert outcome.status == "uploaded"


class TestCheckVariants:
    @pytest.mark.asyncio
    async def test_check_accepts_string_and_uses_token(self):
        engine = SyncEngine(access_key="ak", secret_key="sk")
        engine.dadosgov_token = "tok"
        mock_inv = MagicMock()
        called_origins = []

        async def _collect(origin, datasets=None, **kw):
            called_origins.append(origin)
            return []

        mock_inv.collect = AsyncMock(side_effect=_collect)

        with patch.object(engine, "_require_pysus", return_value=MagicMock()):
            with patch(
                "pysus.management.sync.Inventory", return_value=mock_inv
            ):
                result = await engine.check(datasets="SINAN")

        assert result == {}
        assert "dadosgov" in called_origins


class TestCatalogRowsSaude:
    def test_saude_applies_column_descriptions(self, engine):
        writer = MagicMock()
        writer.ensure_dataset.return_value = 8
        writer.ensure_group.return_value = 3
        writer.get_file.return_value = (99, None)

        central_cursor = MagicMock()
        dataset_cursor = MagicMock()
        columns_cursor = MagicMock()
        f = _remote_file(client_name="saude", basename="arboboletim.jsonl")
        payload = {
            "s3_key": "public/data/saude/x.parquet",
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
            with patch(
                "pysus.api.saude.schemas.apply_column_descriptions"
            ) as mock_apply:
                engine._catalog_rows(
                    central_cursor, dataset_cursor, columns_cursor, f, payload
                )

        mock_apply.assert_called_once()
        assert mock_apply.call_args.args[0] is columns_cursor


class TestCheckpoint:
    @pytest.mark.asyncio
    async def test_checkpoint_timeout_keeps_dirty(self, engine):
        ducklake = MagicMock()
        ducklake.flush_catalogs = AsyncMock(
            side_effect=TimeoutError("slow flush")
        )
        engine._ducklake = ducklake
        engine._changed_catalog = True

        await engine._checkpoint()

        assert engine._changed_catalog is True

    @pytest.mark.asyncio
    async def test_checkpoint_success_clears_dirty(self, engine):
        ducklake = MagicMock()
        ducklake.flush_catalogs = AsyncMock()
        engine._ducklake = ducklake
        engine._changed_catalog = True

        await engine._checkpoint()

        assert engine._changed_catalog is False
        ducklake.flush_catalogs.assert_awaited_once()


class TestUploadFile:
    def _ducklake(self, close_raises=False):
        ducklake = MagicMock()
        ducklake.catalog_adapter = _adapter(close_raises)
        ducklake.columns_adapter = _adapter(close_raises)
        ducklake.get_dataset_adapter.return_value = _adapter(close_raises)
        return ducklake

    def _writer(self):
        writer = MagicMock()
        writer._ensure_management_columns = MagicMock()
        return writer

    @pytest.mark.asyncio
    async def test_requires_ducklake(self, engine):
        f = _remote_file()
        with pytest.raises(Exception, match="not connected"):
            await engine.upload_file(f)

    @pytest.mark.asyncio
    async def test_skips_when_current(self, engine, tmp_path):
        engine._ducklake = self._ducklake()
        f = _remote_file(modify=datetime(2026, 1, 1))

        writer = self._writer()
        writer.get_file_full.return_value = (
            1,
            datetime(2026, 2, 1),
            None,
            None,
            None,
        )

        with patch.object(
            SyncEngine, "writer", new_callable=PropertyMock
        ) as prop:
            prop.return_value = writer
            result = await engine.upload_file(f)
        assert result is False
        writer.get_file_full.assert_called_once()

    @pytest.mark.asyncio
    async def test_content_identical_touches_metadata(self, engine, tmp_path):
        raw_path = tmp_path / "X.dbc"
        raw_path.write_bytes(b"same-bytes")
        raw_digest = sha256_of(raw_path)

        engine._ducklake = self._ducklake()
        engine._download_raw_with_retry = AsyncMock(return_value=raw_path)

        f = _remote_file(modify=datetime(2026, 5, 1))
        writer = self._writer()
        writer.get_file_full.return_value = (
            1,
            datetime(2026, 1, 1),
            None,
            None,
            raw_digest,
        )

        with patch.object(
            SyncEngine, "writer", new_callable=PropertyMock
        ) as prop:
            prop.return_value = writer
            result = await engine.upload_file(f)

        assert result is False
        writer.touch_file.assert_called_once()
        assert not raw_path.exists()

    @pytest.mark.asyncio
    async def test_parquet_identical_touches_metadata(self, engine, tmp_path):
        raw_path = tmp_path / "X.dbc"
        raw_path.write_bytes(b"raw-content")
        parquet_path = tmp_path / "X.parquet"
        parquet_path.write_bytes(b"parquet-content")
        parquet_digest = sha256_of(parquet_path)

        engine._ducklake = self._ducklake()
        engine._download_raw_with_retry = AsyncMock(return_value=raw_path)

        local_file = MagicMock()
        local_file.to_parquet = AsyncMock(
            return_value=MagicMock(path=parquet_path)
        )

        f = _remote_file(modify=datetime(2026, 5, 1))
        writer = self._writer()
        writer.get_file_full.return_value = (
            1,
            datetime(2026, 1, 1),
            None,
            parquet_digest,
            "0" * 64,
        )

        with patch.object(
            SyncEngine, "writer", new_callable=PropertyMock
        ) as prop:
            prop.return_value = writer
            with patch(
                "pysus.api.extensions.ExtensionFactory",
                MagicMock(instantiate=AsyncMock(return_value=local_file)),
            ):
                result = await engine.upload_file(f)

        assert result is False
        writer.touch_file.assert_called_once()
        assert not raw_path.exists()
        assert not parquet_path.exists()

    @pytest.mark.asyncio
    async def test_rejects_file_without_to_parquet(self, engine, tmp_path):
        raw_path = tmp_path / "X.dbc"
        raw_path.write_bytes(b"raw")
        parquet_path = tmp_path / "X.parquet"
        parquet_path.write_bytes(b"pq")

        engine._ducklake = self._ducklake()
        engine._download_raw_with_retry = AsyncMock(return_value=raw_path)

        local_file = MagicMock(spec=["path"])
        local_file.path = raw_path
        f = _remote_file()
        writer = self._writer()
        writer.get_file_full.return_value = None

        with patch.object(
            SyncEngine, "writer", new_callable=PropertyMock
        ) as prop:
            prop.return_value = writer
            with patch(
                "pysus.api.extensions.ExtensionFactory",
                MagicMock(instantiate=AsyncMock(return_value=local_file)),
            ):
                with pytest.raises(RuntimeError, match="cannot convert"):
                    await engine.upload_file(f)

        assert not raw_path.exists()

    @pytest.mark.asyncio
    async def test_full_upload(self, engine, tmp_path):
        raw_path = tmp_path / "X.dbc"
        raw_path.write_bytes(b"raw-content")
        parquet_path = tmp_path / "X.parquet"
        parquet_path.write_bytes(b"parquet-content")

        engine._ducklake = self._ducklake()
        engine._download_raw_with_retry = AsyncMock(return_value=raw_path)
        engine._catalog_rows = MagicMock()

        local_file = MagicMock()
        parquet_file = MagicMock()
        parquet_file.path = parquet_path
        parquet_file.rows = 7
        parquet_file.schema = {"a": "int"}

        async def _to_parquet(callback=None):
            callback(1, 2)
            return parquet_file

        local_file.to_parquet = _to_parquet

        f = _remote_file()
        writer = self._writer()
        writer.get_file_full.return_value = None

        seen = []

        def _progress(processed, total):
            seen.append((processed, total))

        with patch.object(
            SyncEngine, "writer", new_callable=PropertyMock
        ) as prop:
            prop.return_value = writer
            with patch(
                "pysus.api.extensions.ExtensionFactory",
                MagicMock(instantiate=AsyncMock(return_value=local_file)),
            ):
                with patch(
                    "pysus.management.sync.upload_s3",
                    new=AsyncMock(),
                ) as mock_upload:
                    result = await engine.upload_file(f, callback=_progress)

        assert result is True
        mock_upload.assert_awaited_once()
        engine._catalog_rows.assert_called_once()
        assert seen == [(1, 2)]
        assert not raw_path.exists()
        assert not parquet_path.exists()

    @pytest.mark.asyncio
    async def test_failure_cleans_up_and_reraises(self, engine, tmp_path):
        raw_path = tmp_path / "X.dbc"
        raw_path.write_bytes(b"raw")
        parquet_path = tmp_path / "X.parquet"
        parquet_path.write_bytes(b"pq")

        engine._ducklake = self._ducklake(close_raises=True)
        engine._download_raw_with_retry = AsyncMock(return_value=raw_path)

        local_file = MagicMock()
        parquet_file = MagicMock()
        parquet_file.path = parquet_path
        parquet_file.rows = 7
        parquet_file.schema = {}
        local_file.to_parquet = AsyncMock(return_value=parquet_file)

        f = _remote_file()
        writer = self._writer()
        writer.get_file_full.return_value = None

        with patch.object(
            SyncEngine, "writer", new_callable=PropertyMock
        ) as prop:
            prop.return_value = writer
            with patch(
                "pysus.api.extensions.ExtensionFactory",
                MagicMock(instantiate=AsyncMock(return_value=local_file)),
            ):
                with patch(
                    "pysus.management.sync.upload_s3",
                    new=AsyncMock(side_effect=RuntimeError("s3 down")),
                ):
                    with pytest.raises(RuntimeError, match="s3 down"):
                        await engine.upload_file(f)

        assert not raw_path.exists()
        assert not parquet_path.exists()


class TestPidAliveBranches:
    def test_posix_permission_error_means_alive(self):
        with patch("os.name", "posix"):
            with patch("os.kill", side_effect=PermissionError):
                assert SyncEngine._pid_alive(12345) is True

    def test_posix_oserror_means_dead(self):
        with patch("os.name", "posix"):
            with patch("os.kill", side_effect=OSError("invalid pid")):
                assert SyncEngine._pid_alive(12345) is False

    def test_windows_no_windll_means_dead(self):
        with patch("os.name", "nt"):
            with patch("ctypes.windll", create=True, new=None):
                assert SyncEngine._pid_alive(12345) is False

    def test_windows_open_process_failure_means_dead(self):
        kernel32 = MagicMock()
        kernel32.OpenProcess.return_value = None
        with patch("os.name", "nt"):
            with patch(
                "ctypes.windll", create=True, new=MagicMock(kernel32=kernel32)
            ):
                assert SyncEngine._pid_alive(12345) is False

    def test_windows_open_process_success_means_alive(self):
        kernel32 = MagicMock()
        kernel32.OpenProcess.return_value = 5
        with patch("os.name", "nt"):
            with patch(
                "ctypes.windll", create=True, new=MagicMock(kernel32=kernel32)
            ):
                assert SyncEngine._pid_alive(12345) is True
        kernel32.CloseHandle.assert_called_once()

    def test_windows_exception_means_dead(self):
        kernel32 = MagicMock()
        kernel32.OpenProcess.side_effect = RuntimeError("boom")
        with patch("os.name", "nt"):
            with patch(
                "ctypes.windll", create=True, new=MagicMock(kernel32=kernel32)
            ):
                assert SyncEngine._pid_alive(12345) is False


class TestSyncLockBranches:
    def test_corrupt_lock_file_treated_as_dead(
        self, engine, tmp_path, monkeypatch
    ):
        monkeypatch.setattr("pysus.management.sync.CACHEPATH", tmp_path)
        lock = tmp_path / "ducklake" / ".sync.lock"
        lock.parent.mkdir(parents=True, exist_ok=True)
        lock.write_text("not-a-pid")
        engine._acquire_sync_lock()
        assert int(lock.read_text()) > 0

    def test_release_ignores_oserror(self, engine, tmp_path, monkeypatch):
        monkeypatch.setattr("pysus.management.sync.CACHEPATH", tmp_path)
        with patch("pathlib.Path.unlink", side_effect=OSError("boom")):
            engine._release_sync_lock()

    @pytest.mark.asyncio
    async def test_enter_default_creates_pysus_and_takes_lock(
        self, tmp_path, monkeypatch
    ):
        engine = SyncEngine()
        fake_pysus = MagicMock()
        fake_pysus.__aenter__ = AsyncMock()
        ducklake = MagicMock()
        fake_pysus.get_ducklake = AsyncMock(return_value=ducklake)

        monkeypatch.setattr("pysus.management.sync.CACHEPATH", tmp_path)
        with patch("pysus.api.client.PySUS", return_value=fake_pysus):
            result = await engine.__aenter__()

        assert result is engine
        assert engine.pysus is fake_pysus
        assert engine._ducklake is ducklake
        lock = tmp_path / "ducklake" / ".sync.lock"
        assert lock.read_text().strip() == str(os.getpid())


class TestCleanupStaleTmp:
    def test_no_tmp_dir_is_noop(self, tmp_path, monkeypatch):
        monkeypatch.setattr("pysus.management.sync.CACHEPATH", tmp_path)
        SyncEngine._cleanup_stale_tmp()

    def test_removes_extract_dirs_and_files(self, tmp_path, monkeypatch):
        tmp = tmp_path / "management" / "tmp"
        extract = tmp / "X.tmp_extract"
        extract.mkdir(parents=True)
        (extract / "inner").write_bytes(b"x")
        (tmp / "X.csv").write_bytes(b"x")
        monkeypatch.setattr("pysus.management.sync.CACHEPATH", tmp_path)

        SyncEngine._cleanup_stale_tmp()

        assert not extract.exists()
        assert not (tmp / "X.csv").exists()

    def test_ignores_oserrors(self, tmp_path, monkeypatch):
        tmp = tmp_path / "management" / "tmp"
        extract = tmp / "X.tmp_extract"
        extract.mkdir(parents=True)
        (tmp / "X.csv").write_bytes(b"x")
        monkeypatch.setattr("pysus.management.sync.CACHEPATH", tmp_path)

        with patch("shutil.rmtree", side_effect=OSError("busy")):
            with patch("pathlib.Path.unlink", side_effect=OSError("busy")):
                SyncEngine._cleanup_stale_tmp()

        assert extract.is_dir()
        assert (tmp / "X.csv").exists()


class TestDownloadOnceFallback:
    @pytest.mark.asyncio
    async def test_fallback_callback_invoked(self, engine, tmp_path):
        f = _remote_file()
        f.client.ftp = None
        out = tmp_path / "x.dbc"

        async def _download(output=None, callback=None):
            callback(10, 20)
            return output

        f._download = _download
        result = await engine._download_once(f, out)

        assert result == out
        assert f.basename in ("X.dbc",)

    @pytest.mark.asyncio
    async def test_pooled_failure_quits_quietly(self, engine, tmp_path):
        ftp = MagicMock()
        ftp.size.side_effect = OSError("broken")
        ftp.quit.side_effect = OSError("quit also fails")
        client = MagicMock()
        client.ftp = ftp
        f = _remote_file()
        out = tmp_path / "x.dbc"

        with patch(
            "anyio.to_thread.run_sync",
            new=AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a)),
        ):
            with pytest.raises(OSError):
                await engine._download_once(f, out, ftp_client=client)


class TestMiscBranches:
    def test_s3_is_stale_without_s3_record(self):
        ftp = _record("ftp", "DENGBR25.dbc", size=100, file=MagicMock())
        c = FileComparison(key=ftp.identity_key(), records=[ftp])
        assert SyncEngine._s3_is_stale(c) is False

    def test_resume_covers_uncovered_key(self):
        key = _record("ftp", "DENGBR25.dbc", file=MagicMock()).identity_key()
        other = _record("ftp", "OTHER25.dbc", file=MagicMock()).identity_key()
        assert SyncEngine._resume_covers({key: {"ftp"}}, other, "ftp") is False
        assert SyncEngine._resume_covers({key: {"*"}}, key, "ftp") is True
        assert SyncEngine._resume_covers({key: {"ftp"}}, key, "ftp") is True
        assert SyncEngine._resume_covers(None, key, "ftp") is True

    @pytest.mark.asyncio
    async def test_dedupe_swallows_transaction_errors(self, engine):
        from datetime import datetime

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

        adapter = MagicMock()
        adapter.transaction.side_effect = RuntimeError("lock busy")
        ducklake = MagicMock()
        ducklake.get_dataset_adapter.return_value = adapter
        engine._ducklake = ducklake

        import boto3

        with patch.object(boto3, "client", return_value=MagicMock()):
            await engine._dedupe_s3_artifacts([older, newer])

        adapter.transaction.assert_called()


class TestRunPipelineBranches:
    def _ducklake_run(self):
        ducklake = MagicMock()
        ducklake.catalog_adapter.ensure_connected = AsyncMock()
        ducklake.catalog_adapter.connect = AsyncMock()
        ducklake.columns_adapter.ensure_connected = AsyncMock()
        ducklake.columns_adapter.connect = AsyncMock()

        class _Ctx:
            def __init__(self):
                self.conn = MagicMock()

            def __enter__(self):
                return self.conn, self.conn

            def __exit__(self, *_):
                return False

        ducklake.catalog_adapter.transaction = MagicMock(return_value=_Ctx())
        ducklake.columns_adapter.transaction = MagicMock(return_value=_Ctx())
        dataset_adapter = MagicMock(
            ensure_connected=AsyncMock(),
            transaction=MagicMock(return_value=_Ctx()),
            mark_dirty=MagicMock(),
        )
        ducklake.get_dataset_adapter.return_value = dataset_adapter
        return ducklake

    def _writer_run(self):
        writer = MagicMock()
        writer.ensure_dataset.return_value = 1
        writer.ensure_group.return_value = 1
        writer.get_file.return_value = (1, None)
        writer.link_columns = MagicMock()
        writer.upsert_file = MagicMock()
        writer._ensure_management_columns = MagicMock()
        return writer

    def _payload(self):
        return {
            "s3_key": "public/data/k.parquet",
            "size": 1,
            "rows": 1,
            "schema": MagicMock(),
            "raw_digest": "a" * 64,
            "parquet_digest": "b" * 64,
        }

    def _inventory(self, records):
        mock_inv = MagicMock()
        mock_inv.collect = AsyncMock(
            side_effect=lambda origin, **kw: records.get(origin, [])
        )
        return mock_inv

    @pytest.mark.asyncio
    async def test_dry_run_reports_needs_update(self, engine):
        pni = FileRecord(
            origin="ducklake",
            dataset="PNI",
            name="PNIBR25.parquet",
            path="public/data/ftp/pni/DENG/2025/_/BR/PNIBR25.parquet",
            year=2025,
        )
        ftp = _record("ftp", "DENGBR25.dbc", size=100, file=MagicMock())
        records = {
            "ducklake": [pni],
            "ftp": [ftp],
            "dadosgov": [],
            "saude": [],
        }
        engine._ducklake = self._ducklake_run()

        with patch.object(engine, "_require_pysus", return_value=MagicMock()):
            with patch(
                "pysus.management.sync.Inventory",
                return_value=self._inventory(records),
            ):
                report = await engine.run(
                    datasets=["SINAN"],
                    dry_run=True,
                    force=True,
                )

        assert report.summary()["needs_update"] == 2
        assert report.summary()["total"] == 2

    @pytest.mark.asyncio
    async def test_run_gov_worker_vacinacao_and_skips(self, engine, tmp_path):
        gov = _record(
            "dadosgov",
            "GOVBR25.csv.zip",
            file=_remote_file(basename="GOVBR25.csv.zip"),
        )
        gov2 = _record(
            "dadosgov",
            "GOV2BR25.csv.zip",
            file=_remote_file(basename="GOV2BR25.csv.zip"),
        )
        vac = _record(
            "dadosgov",
            "VACBR25.csv.zip",
            dataset="VACINACAO",
            size=0,
            file=_remote_file(basename="VACBR25.csv.zip"),
        )
        vacok = _record(
            "dadosgov",
            "VACOKBR25.csv.zip",
            dataset="VACINACAO",
            size=0,
            file=_remote_file(basename="VACOKBR25.csv.zip"),
        )
        ftp = _record("ftp", "DENGBR25.dbc", size=100, file=MagicMock())
        mirror = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="public/data/ftp/sinan/DENG/2025/_/BR/DENGBR25.parquet",
            year=2025,
            size=100,
            source_size=100,
        )
        pni = FileRecord(
            origin="ducklake",
            dataset="PNI",
            name="PNIBR25.parquet",
            path="public/data/ftp/pni/DENG/2025/_/BR/PNIBR25.parquet",
            year=2025,
        )
        records = {
            "ducklake": [mirror, pni],
            "ftp": [ftp],
            "dadosgov": [gov, gov2, vac, vacok],
            "saude": [],
        }
        raw = tmp_path / "raw.bin"
        raw.write_bytes(b"x")

        engine.dadosgov_token = "tok"
        engine._ducklake = self._ducklake_run()

        calls: dict[str, int] = {}

        async def _download(file, ftp_client=None):
            calls[file.basename] = calls.get(file.basename, 0) + 1
            if file.basename == "GOVBR25.csv.zip" and calls[file.basename] == 1:
                raise RuntimeError("gov download boom")
            return raw

        async def _convert(file, raw, callback=None):
            if file.basename == "VACBR25.csv.zip":
                raise RuntimeError("vac conversion boom")
            return self._payload()

        engine._download_raw_with_retry = AsyncMock(side_effect=_download)
        engine._convert_and_upload = AsyncMock(side_effect=_convert)

        journal = tmp_path / "journal.jsonl"
        outcomes = []

        with patch.object(engine, "_require_pysus", return_value=MagicMock()):
            with patch(
                "pysus.management.sync.Inventory",
                return_value=self._inventory(records),
            ):
                with patch.object(engine, "_checkpoint", new=AsyncMock()):
                    with patch.object(
                        SyncEngine,
                        "writer",
                        new_callable=PropertyMock,
                    ) as mock_writer_prop:
                        mock_writer_prop.return_value = self._writer_run()
                        report = await engine.run(
                            datasets=["SINAN"],
                            checkpoint_every=1,
                            on_outcome=outcomes.append,
                            journal=journal,
                        )

        assert report.summary()["uploaded"] == 3
        assert report.summary()["failed"] == 1
        assert report.summary()["skipped"] == 2
        assert calls.get("GOVBR25.csv.zip") == 2
        assert outcomes
        assert journal.exists()

    @pytest.mark.asyncio
    async def test_run_retries_failed_ftp_and_catalog_errors(
        self, engine, tmp_path
    ):
        bad = _record(
            "ftp",
            "BADBR25.dbc",
            size=100,
            file=_remote_file(basename="BADBR25.dbc"),
        )
        crash = _record(
            "ftp",
            "CRASHBR25.dbc",
            size=100,
            file=_remote_file(basename="CRASHBR25.dbc"),
        )
        once = _record(
            "ftp",
            "ONCEBR25.dbc",
            size=100,
            file=_remote_file(basename="ONCEBR25.dbc"),
        )
        conv = _record(
            "ftp",
            "CONVBR25.dbc",
            size=100,
            file=_remote_file(basename="CONVBR25.dbc"),
        )
        records = {
            "ducklake": [],
            "ftp": [bad, crash, once, conv],
            "dadosgov": [],
            "saude": [],
        }
        raw = tmp_path / "raw.bin"
        raw.write_bytes(b"x")

        engine._ducklake = self._ducklake_run()

        calls: dict[str, int] = {}

        async def _download(file, ftp_client=None):
            calls[file.basename] = calls.get(file.basename, 0) + 1
            if file.basename == "BADBR25.dbc":
                raise RuntimeError("download boom")
            if file.basename == "ONCEBR25.dbc" and calls[file.basename] == 1:
                raise RuntimeError("download boom")
            return raw

        async def _convert(file, raw, callback=None):
            if file.basename == "CONVBR25.dbc":
                raise RuntimeError("convert boom")
            return self._payload()

        engine._download_raw_with_retry = AsyncMock(side_effect=_download)
        engine._convert_and_upload = AsyncMock(side_effect=_convert)

        def _catalog_write_entry(adapter, central, columns, file, payload):
            if file.basename == "CRASHBR25.dbc":
                raise RuntimeError("catalog boom")

        engine._catalog_write_entry = MagicMock(
            side_effect=_catalog_write_entry
        )
        client = MagicMock()
        client.connect = AsyncMock()
        client.ftp = MagicMock()

        with patch.object(engine, "_require_pysus", return_value=MagicMock()):
            with patch(
                "pysus.management.sync.Inventory",
                return_value=self._inventory(records),
            ):
                with patch("pysus.api.ftp.client.FTP", return_value=client):
                    with patch.object(
                        SyncEngine,
                        "writer",
                        new_callable=PropertyMock,
                    ) as mock_writer_prop:
                        mock_writer_prop.return_value = self._writer_run()
                        report = await engine.run(
                            datasets=["SINAN"],
                            ftp_connections=2,
                        )

        assert report.summary()["uploaded"] == 1
        assert report.summary()["failed"] == 3
        assert calls.get("ONCEBR25.dbc") == 2

    @pytest.mark.asyncio
    async def test_force_reprocesses_ducklake_only(self, engine):
        from pysus.management.records import SyncOutcome

        pni = FileRecord(
            origin="ducklake",
            dataset="PNI",
            name="PNIBR25.parquet",
            path="public/data/ftp/pni/DENG/2025/_/BR/PNIBR25.parquet",
            year=2025,
        )
        records = {
            "ducklake": [pni],
            "ftp": [],
            "dadosgov": [],
            "saude": [],
        }
        engine._ducklake = self._ducklake_run()

        outcome = SyncOutcome(
            key=pni.identity_key(),
            origin="ducklake",
            status="uploaded",
        )

        with patch.object(engine, "_require_pysus", return_value=MagicMock()):
            with patch(
                "pysus.management.sync.Inventory",
                return_value=self._inventory(records),
            ):
                with patch.object(
                    engine, "_reprocess", new=AsyncMock(return_value=outcome)
                ):
                    report = await engine.run(
                        datasets=["PNI"],
                        force=True,
                    )

        assert report.summary()["uploaded"] == 1


class TestRequiredHelpers:
    def test_require_pysus_returns_connected_client(self, engine):
        pysus = MagicMock()
        engine.pysus = pysus
        assert engine._require_pysus() is pysus

    def test_require_pysus_raises_when_disconnected(self, engine):
        from pysus.api.errors import ConnectionError as PysusConnectionError

        engine.pysus = None
        with pytest.raises(PysusConnectionError):
            engine._require_pysus()

    def test_require_ducklake_returns_connected(self, engine):
        ducklake = MagicMock()
        engine._ducklake = ducklake
        assert engine._require_ducklake() is ducklake

    def test_require_ducklake_raises_when_disconnected(self, engine):
        from pysus.api.errors import ConnectionError as PysusConnectionError

        with pytest.raises(PysusConnectionError):
            engine._require_ducklake()


class TestCollectWithRetry:
    @pytest.mark.asyncio
    async def test_retries_transient_collect_errors(self, engine):
        import httpx

        engine.dadosgov_token = "tok"
        engine._ducklake = self._ducklake_run()

        inv = MagicMock()
        inv.collect = AsyncMock(
            side_effect=[
                httpx.ConnectError("dns"),
                httpx.ConnectError("dns"),
                [],
            ]
        )
        with patch.object(engine, "_require_pysus", return_value=MagicMock()):
            with patch.object(
                SyncEngine, "inventory", new_callable=PropertyMock
            ) as inv_prop:
                inv_prop.return_value = inv
                with patch("asyncio.sleep", new=AsyncMock()) as mock_sleep:
                    report = await engine.run(
                        datasets=["SINAN"],
                        origins=("ducklake",),
                    )

        assert inv.collect.await_count == 3
        assert mock_sleep.await_count == 2
        assert len(report.outcomes) == 0

    @pytest.mark.asyncio
    async def test_collect_gives_up_after_three_attempts(self, engine):
        import httpx

        engine.dadosgov_token = "tok"
        engine._ducklake = self._ducklake_run()

        inv = MagicMock()
        inv.collect = AsyncMock(side_effect=httpx.ConnectError("dns"))
        with patch.object(engine, "_require_pysus", return_value=MagicMock()):
            with patch.object(
                SyncEngine, "inventory", new_callable=PropertyMock
            ) as inv_prop:
                inv_prop.return_value = inv
                with patch("asyncio.sleep", new=AsyncMock()):
                    with pytest.raises(httpx.ConnectError):
                        await engine.run(
                            datasets=["SINAN"],
                            origins=("ducklake",),
                        )
        assert inv.collect.await_count == 3

    def _ducklake_run(self):
        ducklake = MagicMock()
        ducklake.catalog_adapter.ensure_connected = AsyncMock()
        ducklake.catalog_adapter.connect = AsyncMock()
        ducklake.columns_adapter.ensure_connected = AsyncMock()
        ducklake.columns_adapter.connect = AsyncMock()

        class _Ctx:
            def __init__(self):
                self.conn = MagicMock()

            def __enter__(self):
                return self.conn, self.conn

            def __exit__(self, *_):
                return False

        ducklake.catalog_adapter.transaction = MagicMock(return_value=_Ctx())
        ducklake.columns_adapter.transaction = MagicMock(return_value=_Ctx())
        ducklake.get_dataset_adapter.return_value = MagicMock(
            ensure_connected=AsyncMock(),
            transaction=MagicMock(return_value=_Ctx()),
            mark_dirty=MagicMock(),
        )
        return ducklake


class TestFixMisparsedMetadata:
    """The old SIA formatter glued part suffixes into the month (e.g.
    ``BIRJ2504_2`` -> month 42); repaired rows must be re-keyed."""

    def _record(self, name, month=42, dataset="SINAN"):
        return FileRecord(
            origin="ducklake",
            dataset=dataset,
            name=name,
            path=f"public/data/ftp/sinan/DENG/2025/_/BR/{name}",
            year=2025,
            month=month,
            state="BR",
        )

    @pytest.mark.asyncio
    async def test_repairs_misparsed_rows(self, engine, monkeypatch, tmp_path):
        monkeypatch.setattr("pysus.management.sync.CACHEPATH", tmp_path)
        from pysus.management.records import compose_s3_key
        from pysus.management.sync import SyncEngine

        rec_copy = self._record("BIRX2504_2.parquet")
        rec_alias = self._record("BIRJ2406_2.parquet")
        rec_alias.path = (
            "public/data/ftp/sinan/DENG/2024/_/BR/BIRJ2406_2.parquet"
        )
        rec_alias.year = 2024

        def _formatter(name):
            if name == "BIRX2504_2.parquet":
                return {
                    "group": {"name": "DENG", "long_name": "Dengue"},
                    "month": "4",
                    "year": 2025,
                    "state": "BR",
                }
            return {"group": "DENG", "month": 6, "year": 2024}

        def _formatter_for(origin, dataset):
            return _formatter

        new_key = compose_s3_key(
            origin="ftp",
            dataset="SINAN",
            name="BIRJ2406_2.parquet",
            group="DENG",
            year=2024,
            month=6,
            state="BR",
        )

        s3 = MagicMock()
        s3.head_object.side_effect = [
            RuntimeError("gone"),
            {"Metadata": {"pysus-alias": new_key}},
        ]
        s3.copy_object = MagicMock()
        s3.put_object = MagicMock()

        ducklake = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.side_effect = [None, MagicMock()]
        adapter = MagicMock()
        adapter.transaction.return_value.__enter__ = MagicMock(
            return_value=(cursor, cursor)
        )
        adapter.transaction.return_value.__exit__ = MagicMock(
            return_value=False
        )
        adapter.mark_dirty = MagicMock()
        ducklake.get_dataset_adapter.return_value = adapter
        engine._ducklake = ducklake

        writer = MagicMock()
        writer.ensure_group.return_value = 1

        import boto3

        with patch.object(boto3, "client", return_value=s3):
            with patch.object(
                SyncEngine, "writer", new_callable=PropertyMock
            ) as wp:
                wp.return_value = writer
                with patch(
                    "pysus.management.normalize.formatter_for",
                    side_effect=_formatter_for,
                ):
                    with patch(
                        "pysus.api.ducklake.functional.alias_marker",
                        return_value="alias-marker",
                    ):
                        await engine._fix_misparsed_metadata(
                            [rec_copy, rec_alias]
                        )

        assert s3.copy_object.called
        assert s3.put_object.called
        assert engine._changed_catalog
        assert rec_copy.path != new_key

    @pytest.mark.asyncio
    async def test_skips_unfixable_rows(self, engine):
        records = [
            self._record("BIRZ2501_2.parquet", month=42, dataset="NONEFMT"),
            self._record("BIRZ2502_2.parquet", month=42, dataset="BADMONTH"),
            self._record("BIRZ2503_2.parquet", month=None),
            self._record("BIRZ2504_2.parquet", month=6),
            self._record("BIRZ2505_2.parquet", month=42, dataset="BADFORMAT"),
        ]

        def _formatter_for(origin, dataset):
            if dataset == "NONEFMT":
                return None
            if dataset == "BADMONTH":
                return lambda name: {"month": 99, "year": 2025}
            if dataset == "BADFORMAT":
                return lambda name: (_ for _ in ()).throw(ValueError("bad"))
            return lambda name: {"month": 6, "year": 2025}

        ducklake = MagicMock()
        engine._ducklake = ducklake
        import boto3

        with patch.object(boto3, "client", return_value=MagicMock()):
            with patch(
                "pysus.management.normalize.formatter_for",
                side_effect=_formatter_for,
            ):
                await engine._fix_misparsed_metadata(records)

        assert not engine._changed_catalog

    @pytest.mark.asyncio
    async def test_repair_error_is_logged_not_raised(self, engine):
        rec = self._record("BIRK2505_2.parquet")
        engine._ducklake = MagicMock()
        engine._ducklake.get_dataset_adapter.return_value = MagicMock()

        writer = MagicMock()
        writer.ensure_group.side_effect = RuntimeError("catalog boom")

        import boto3

        with patch.object(boto3, "client", return_value=MagicMock()):
            with patch.object(
                SyncEngine, "writer", new_callable=PropertyMock
            ) as wp:
                wp.return_value = writer
                with patch(
                    "pysus.management.normalize.formatter_for",
                    return_value=lambda name: {
                        "group": "DENG",
                        "month": 5,
                        "year": 2025,
                    },
                ):
                    await engine._fix_misparsed_metadata([rec])

        assert not engine._changed_catalog


class TestConvertAndUploadReal:
    @pytest.mark.asyncio
    async def test_full_convert_and_upload_pipeline(self, engine, tmp_path):
        raw_path = tmp_path / "X.dbc"
        raw_path.write_bytes(b"raw-content")
        parquet_path = tmp_path / "X.parquet"
        parquet_path.write_bytes(b"parquet-content")

        local_file = MagicMock()
        parquet_file = MagicMock()
        parquet_file.path = parquet_path
        parquet_file.rows = 7
        parquet_file.schema = {"a": "int"}

        async def _to_parquet(callback=None):
            callback(1, 2)
            return parquet_file

        local_file.to_parquet = _to_parquet
        f = _remote_file()
        seen = []

        def _progress(processed, total):
            seen.append((processed, total))

        with patch(
            "pysus.api.extensions.ExtensionFactory",
            MagicMock(instantiate=AsyncMock(return_value=local_file)),
        ):
            with patch(
                "pysus.management.sync.upload_s3",
                new=AsyncMock(),
            ):
                payload = await engine._convert_and_upload(
                    f, raw_path, callback=_progress
                )

        assert payload["rows"] == 7
        assert seen == [(1, 2)]


class TestWeightGateFailures:
    """The weight gate is advisory: when it breaks the pipeline must
    keep going with unbounded weights instead of crashing."""

    def _ducklake(self):
        ducklake = MagicMock()
        ducklake.catalog_adapter.ensure_connected = AsyncMock()
        ducklake.catalog_adapter.connect = AsyncMock()
        ducklake.columns_adapter.ensure_connected = AsyncMock()
        ducklake.columns_adapter.connect = AsyncMock()

        class _Ctx:
            def __init__(self):
                self.conn = MagicMock()

            def __enter__(self):
                return self.conn, self.conn

            def __exit__(self, *_):
                return False

        ducklake.catalog_adapter.transaction = MagicMock(return_value=_Ctx())
        ducklake.columns_adapter.transaction = MagicMock(return_value=_Ctx())
        ducklake.get_dataset_adapter.return_value = MagicMock(
            ensure_connected=AsyncMock(),
            transaction=MagicMock(return_value=_Ctx()),
            mark_dirty=MagicMock(),
        )
        return ducklake

    def _writer(self):
        writer = MagicMock()
        writer.ensure_dataset.return_value = 1
        writer.ensure_group.return_value = 1
        writer.get_file.return_value = (1, None)
        writer.link_columns = MagicMock()
        writer.upsert_file = MagicMock()
        writer._ensure_management_columns = MagicMock()
        return writer

    def _payload(self):
        return {
            "s3_key": "public/data/k.parquet",
            "size": 1,
            "rows": 1,
            "schema": MagicMock(),
            "raw_digest": "a" * 64,
            "parquet_digest": "b" * 64,
        }

    def _inventory(self, records):
        mock_inv = MagicMock()
        mock_inv.collect = AsyncMock(
            side_effect=lambda origin, **kw: records.get(origin, [])
        )
        return mock_inv

    @pytest.mark.asyncio
    async def test_gate_acquire_failures_last_the_run(self, engine, tmp_path):
        from pysus.management.sync import _WeightGate

        class _GateAcquireError(_WeightGate):
            async def acquire(self, weight):
                raise RuntimeError("gate oom")

            async def adjust(self, old, new):
                raise RuntimeError("not reached")

            async def release(self, weight):
                pass

        ftp = _record(
            "ftp",
            "DENGBR25.dbc",
            size=100,
            file=_remote_file(basename="DENGBR25.dbc"),
        )
        fail = _record(
            "ftp",
            "FAILBR25.dbc",
            size=100,
            file=_remote_file(basename="FAILBR25.dbc"),
        )
        gov = _record(
            "dadosgov",
            "GOVBR25.csv.zip",
            file=_remote_file(basename="GOVBR25.csv.zip"),
        )
        vac = _record(
            "dadosgov",
            "VACBR25.csv.zip",
            dataset="VACINACAO",
            size=0,
            file=_remote_file(basename="VACBR25.csv.zip"),
        )
        records = {
            "ducklake": [],
            "ftp": [ftp, fail],
            "dadosgov": [gov, vac],
            "saude": [],
        }
        raw = tmp_path / "raw.bin"
        raw.write_bytes(b"x")

        engine.dadosgov_token = "tok"
        engine._ducklake = self._ducklake()

        async def _download(file, ftp_client=None):
            if file.basename == "FAILBR25.dbc":
                raise RuntimeError("download boom")
            return raw

        async def _convert(file, raw, callback=None):
            return self._payload()

        engine._download_raw_with_retry = AsyncMock(side_effect=_download)
        engine._convert_and_upload = AsyncMock(side_effect=_convert)

        with patch.object(engine, "_require_pysus", return_value=MagicMock()):
            with patch(
                "pysus.management.sync.Inventory",
                return_value=self._inventory(records),
            ):
                with patch(
                    "pysus.management.sync._WeightGate", _GateAcquireError
                ):
                    with patch(
                        "pysus.api.ftp.client.FTP",
                        return_value=MagicMock(
                            connect=AsyncMock(), ftp=MagicMock()
                        ),
                    ) as fcls:
                        fcls.return_value.connect = AsyncMock()
                        with patch.object(
                            SyncEngine,
                            "writer",
                            new_callable=PropertyMock,
                        ) as mock_writer_prop:
                            mock_writer_prop.return_value = self._writer()
                            report = await engine.run(
                                datasets=["SINAN"],
                                ftp_connections=2,
                            )

        assert report.summary()["uploaded"] == 3
        assert report.summary()["failed"] == 1

    @pytest.mark.asyncio
    async def test_gate_adjust_failures_last_the_run(self, engine, tmp_path):
        from pysus.management.sync import _WeightGate

        class _GateAdjustError(_WeightGate):
            async def acquire(self, weight):
                return weight

            async def adjust(self, old, new):
                raise RuntimeError("adjust oom")

            async def release(self, weight):
                pass

        ftp = _record(
            "ftp",
            "DENGBR25.dbc",
            size=100,
            file=_remote_file(basename="DENGBR25.dbc"),
        )
        gov = _record(
            "dadosgov",
            "GOVBR25.csv.zip",
            file=_remote_file(basename="GOVBR25.csv.zip"),
        )
        records = {
            "ducklake": [],
            "ftp": [ftp],
            "dadosgov": [gov],
            "saude": [],
        }
        raw = tmp_path / "raw.bin"
        raw.write_bytes(b"x")

        engine.dadosgov_token = "tok"
        engine._ducklake = self._ducklake()

        async def _download(file, ftp_client=None):
            return raw

        async def _convert(file, raw, callback=None):
            return self._payload()

        engine._download_raw_with_retry = AsyncMock(side_effect=_download)
        engine._convert_and_upload = AsyncMock(side_effect=_convert)

        with patch.object(engine, "_require_pysus", return_value=MagicMock()):
            with patch(
                "pysus.management.sync.Inventory",
                return_value=self._inventory(records),
            ):
                with patch(
                    "pysus.management.sync._WeightGate", _GateAdjustError
                ):
                    with patch(
                        "pysus.api.ftp.client.FTP",
                        return_value=MagicMock(
                            connect=AsyncMock(), ftp=MagicMock()
                        ),
                    ) as fcls:
                        fcls.return_value.connect = AsyncMock()
                        with patch.object(
                            SyncEngine,
                            "writer",
                            new_callable=PropertyMock,
                        ) as mock_writer_prop:
                            mock_writer_prop.return_value = self._writer()
                            report = await engine.run(
                                datasets=["SINAN"],
                                ftp_connections=1,
                            )

        assert report.summary()["uploaded"] == 2
        assert report.summary()["failed"] == 0
