"""Tests for the shared-engine/transaction plumbing in adapters."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pysus.api.ducklake.catalog import adapters as adapters_module
from pysus.api.ducklake.catalog.adapters import (
    CatalogAdapter,
    ColumnsAdapter,
    DatasetAdapter,
)
from pysus.api.errors import CatalogError


class TestRegistry:
    def test_dispose_shared_missing(self):
        adapters_module._dispose_shared(Path("/tmp/nope.duckdb"))

    def test_dispose_shared_existing(self, tmp_path):
        path = tmp_path / "catalog.duckdb"
        engine = MagicMock()
        adapters_module._SHARED_ENGINES[str(path)] = engine
        adapters_module._dispose_shared(path)
        engine.dispose.assert_called_once()
        assert str(path) not in adapters_module._SHARED_ENGINES


class TestBaseAdapterSurface:
    def test_mark_dirty_and_flag(self):
        adapter = CatalogAdapter()
        assert not adapter.local_dirty
        adapter.mark_dirty()
        assert adapter.local_dirty

    def test_connected_false_initially(self):
        assert not CatalogAdapter().connected

    def test_raw_connection_not_connected(self):
        with pytest.raises(CatalogError, match="not initialized"):
            CatalogAdapter().raw_connection()

    def test_checkpoint_uses_shared_engine(self, tmp_path, monkeypatch):
        monkeypatch.setattr(adapters_module, "CACHEPATH", tmp_path)
        adapter = CatalogAdapter()
        engine = MagicMock()
        engine.raw_connection.return_value.execute = MagicMock()
        adapters_module._SHARED_ENGINES[str(adapter.db_local.resolve())] = (
            engine
        )
        adapter.checkpoint()
        engine.raw_connection.return_value.execute.assert_called_once_with(
            "CHECKPOINT"
        )

    @pytest.mark.asyncio
    async def test_ensure_connected_creates_engine(self, tmp_path, monkeypatch):
        monkeypatch.setattr(adapters_module, "CACHEPATH", tmp_path)
        adapter = CatalogAdapter()
        engine = MagicMock()
        with patch.object(adapter, "connect", new=AsyncMock()) as mock_connect:
            with patch.object(
                adapter,
                "setup_engine",
                return_value=engine,
            ):
                await adapter.ensure_connected()
        mock_connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ensure_connected_engine_exists(self, tmp_path, monkeypatch):
        monkeypatch.setattr(adapters_module, "CACHEPATH", tmp_path)
        adapter = CatalogAdapter()
        adapter._engine = MagicMock()
        with patch.object(adapter, "connect", new=AsyncMock()) as mock:
            await adapter.ensure_connected()
        mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reconnect_disposes_and_reconnects(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(adapters_module, "CACHEPATH", tmp_path)
        adapter = CatalogAdapter()
        with patch.object(adapter, "connect", new=AsyncMock()) as mock_connect:
            with patch.object(
                adapters_module, "_dispose_shared"
            ) as mock_dispose:
                await adapter.reconnect()
        mock_dispose.assert_called_once()
        mock_connect.assert_awaited_once_with(force=True)


class TestTransaction:
    def test_transaction_commits(self, tmp_path):
        import duckdb

        path = tmp_path / "catalog.duckdb"
        con = duckdb.connect(str(path))
        con.execute("CREATE TABLE t (x INTEGER)")
        con.close()

        adapter = DatasetAdapter(name="x", dataset_id=1, engine=None)
        adapter.db_local = path
        engine = MagicMock()
        raw_conn = duckdb.connect(str(path))
        engine.raw_connection.return_value = raw_conn

        with patch.object(adapter, "setup_engine", return_value=engine):
            with adapter.transaction() as (conn, cursor):
                cursor.execute("INSERT INTO t VALUES (42)")
                conn.commit()

        check = duckdb.connect(str(path), read_only=True)
        rows = check.execute("SELECT * FROM t").fetchall()
        check.close()
        assert rows == [(42,)]

    def test_transaction_broken_raises(self, tmp_path):
        adapter = DatasetAdapter(name="x", dataset_id=1, engine=None)
        adapter.db_local = tmp_path / "catalog.duckdb"
        engine = MagicMock()
        probe = MagicMock()
        probe.execute.side_effect = RuntimeError("closed")
        broken = MagicMock()
        broken.cursor.return_value = probe
        engine.raw_connection.return_value = broken

        with patch.object(adapter, "setup_engine", return_value=engine):
            with pytest.raises(CatalogError, match="broken"):
                with adapter.transaction():
                    pass


class TestAdapterKinds:
    def test_adapter_paths(self, tmp_path, monkeypatch):
        monkeypatch.setattr(adapters_module, "CACHEPATH", tmp_path)
        assert CatalogAdapter().db_remote == Path("public/catalog.duckdb")
        assert ColumnsAdapter().db_remote == Path(
            "public/catalog_columns.duckdb"
        )
        ds = DatasetAdapter(name="sinan", dataset_id=8)
        assert ds.db_remote == Path("public/catalog_sinan.duckdb")
        assert ds.dataset_name == "sinan"

    def test_upload_catalog_missing_credentials(self, tmp_path, monkeypatch):
        monkeypatch.setattr(adapters_module, "CACHEPATH", tmp_path)
        adapter = CatalogAdapter()
        adapter.db_local = tmp_path / "catalog.duckdb"
        adapter.db_local.write_bytes(b"")
        with pytest.raises(PermissionError, match="credentials"):
            import asyncio

            asyncio.run(adapter._upload_catalog())

    def test_upload_catalog_missing_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(adapters_module, "CACHEPATH", tmp_path)
        adapter = CatalogAdapter()
        adapter.db_local = tmp_path / "missing.duckdb"
        adapter.credentials = MagicMock()
        adapter.checkpoint = MagicMock()
        with pytest.raises(FileNotFoundError):
            import asyncio

            asyncio.run(adapter._upload_catalog())

    @pytest.mark.asyncio
    async def test_upload_catalog_calls_s3(self, tmp_path, monkeypatch):
        monkeypatch.setattr(adapters_module, "CACHEPATH", tmp_path)
        adapter = CatalogAdapter()
        adapter.db_local = tmp_path / "catalog.duckdb"
        adapter.db_local.write_bytes(b"x")
        creds = MagicMock()
        creds.access_key.get_secret_value.return_value = "ak"
        creds.secret_key.get_secret_value.return_value = "sk"
        adapter.credentials = creds
        adapter.checkpoint = MagicMock()
        with patch.object(
            adapters_module, "upload_s3", new=AsyncMock()
        ) as mock_upload:
            await adapter._upload_catalog()
        mock_upload.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_clears_refs_only(self, tmp_path, monkeypatch):
        monkeypatch.setattr(adapters_module, "CACHEPATH", tmp_path)
        adapter = CatalogAdapter()
        engine = MagicMock()
        adapters_module._SHARED_ENGINES[str(adapter.db_local.resolve())] = (
            engine
        )
        adapter._engine = engine
        adapter._session_factory = MagicMock()
        await adapter.close()
        engine.dispose.assert_not_called()
        assert adapter._engine is None

    @pytest.mark.asyncio
    async def test_sync_uploads_dirty_keeps_engine(self, tmp_path, monkeypatch):
        monkeypatch.setattr(adapters_module, "CACHEPATH", tmp_path)
        adapter = CatalogAdapter()
        adapters_module._SHARED_ENGINES[str(adapter.db_local.resolve())] = (
            MagicMock()
        )
        adapter._engine = MagicMock()
        adapter._session_factory = MagicMock()
        adapter._local_dirty = True
        creds = MagicMock()
        creds.access_key.get_secret_value.return_value = "ak"
        creds.secret_key.get_secret_value.return_value = "sk"
        adapter.credentials = creds
        adapter.db_local.write_bytes(b"x")
        adapter.checkpoint = MagicMock()
        with patch.object(
            adapters_module, "upload_s3", new=AsyncMock()
        ) as mock_upload:
            await adapter.sync(update=True)
        mock_upload.assert_awaited_once()
        assert not adapter.local_dirty
        assert adapter._engine is not None
        assert adapter._session_factory is not None

    @pytest.mark.asyncio
    async def test_sync_noop_when_clean(self, tmp_path, monkeypatch):
        monkeypatch.setattr(adapters_module, "CACHEPATH", tmp_path)
        adapter = CatalogAdapter()
        adapter._engine = MagicMock()
        adapter._local_dirty = False
        with patch.object(
            adapter, "_upload_catalog", new=AsyncMock()
        ) as mock_upload:
            await adapter.sync(update=True)
        mock_upload.assert_not_awaited()
        assert not adapter.local_dirty
        assert adapter._engine is not None

    @pytest.mark.asyncio
    async def test_sync_keeps_dirty_on_failure(self, tmp_path, monkeypatch):
        monkeypatch.setattr(adapters_module, "CACHEPATH", tmp_path)
        adapter = CatalogAdapter()
        adapter._engine = MagicMock()
        adapter._local_dirty = True
        with patch.object(
            adapter,
            "_upload_catalog",
            new=AsyncMock(side_effect=OSError("boom")),
        ) as mock_upload:
            await adapter.sync(update=True)
        mock_upload.assert_awaited_once()
        assert adapter.local_dirty
        assert adapter._engine is not None
