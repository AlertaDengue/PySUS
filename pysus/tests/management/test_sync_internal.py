"""Tests for pysus.management.sync connection and helper paths."""

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
            new=AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw)),
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
        file._download = AsyncMock()
        out = tmp_path / "x.dbc"
        await engine._download_once(file, out)
        file._download.assert_awaited_once_with(output=out)


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
