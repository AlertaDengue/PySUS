"""Tests for the synchronous PySUS context manager (``with PySUS() as c:``)."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pysus.api.client import PySUS, _run_sync, _sync_aware


class TestRunSync:
    """Tests for the standalone ``_run_sync`` helper."""

    def test_runs_coroutine_in_normal_context(self):
        async def coro():
            return 42

        assert _run_sync(coro()) == 42

    def test_raises_when_no_nest_asyncio_in_running_loop(self):
        async def _inner():
            saved = __import__("sys").modules.pop("nest_asyncio", None)
            import builtins

            real_import = builtins.__import__

            def _raise(name, *a, **kw):
                if name == "nest_asyncio":
                    raise ImportError("no nest_asyncio")
                return real_import(name, *a, **kw)

            try:
                with patch("builtins.__import__", side_effect=_raise):
                    with pytest.raises(RuntimeError, match="nest_asyncio"):
                        _run_sync(asyncio.sleep(0))
            finally:
                if saved is not None:
                    __import__("sys").modules["nest_asyncio"] = saved

        asyncio.run(_inner())


class TestSyncAwareDecorator:
    """Tests for the ``@_sync_aware`` decorator."""

    def test_async_mode_returns_coroutine(self):
        class Obj:
            _sync = False

            @_sync_aware
            async def method(self):
                return 99

        obj = Obj()
        result = obj.method()
        assert asyncio.iscoroutine(result)
        assert asyncio.run(result) == 99

    def test_sync_mode_returns_value_directly(self):
        class Obj:
            _sync = True
            _sync_running = False
            _loop = None

            def _run_async(self, coro):
                return asyncio.run(coro)

            @_sync_aware
            async def method(self):
                return 99

        obj = Obj()
        result = obj.method()
        assert result == 99

    def test_sync_mode_nested_uses_coroutine(self):
        """When _sync_running is True, the decorator returns a coroutine
        for the caller to await normally."""
        results = []

        async def _run(coro):
            return await coro

        class Obj:
            _sync = True
            _sync_running = True
            _loop = None

            def _run_async(self, coro):
                return asyncio.run(coro)

            @_sync_aware
            async def inner(self):
                results.append("inner")
                return "result"

            @_sync_aware
            async def outer(self):
                val = await self.inner()
                results.append("outer")
                return val

        obj = Obj()
        coro = obj.outer()
        assert asyncio.iscoroutine(coro)
        asyncio.run(coro)
        assert results == ["inner", "outer"]


class TestPySUSSyncContext:
    """Tests for ``with PySUS() as client:`` synchronous usage."""

    def test_enter_sets_sync_flag(self, tmp_path):
        db = tmp_path / "test.db"
        with PySUS(db_path=db) as client:
            assert client._sync is True

    def test_exit_resets_sync_flag(self, tmp_path):
        db = tmp_path / "test.db"
        client = PySUS(db_path=db)
        with client:
            assert client._sync is True
        assert client._sync is False

    def test_sync_query_returns_value(self, tmp_path):
        db = tmp_path / "test.db"
        client = PySUS(db_path=db)
        mock_file = MagicMock()
        with client as c:
            with patch.object(
                c,
                "_run_async",
                return_value=[mock_file],
            ) as mock_run:
                result = c.query(dataset="sinan", year=2024)
                assert result == [mock_file]
                mock_run.assert_called_once()

    def test_sync_download_returns_value(self, tmp_path):
        db = tmp_path / "test.db"
        client = PySUS(db_path=db)
        mock_local = MagicMock()
        with client as c:
            with patch.object(
                c,
                "_run_async",
                return_value=mock_local,
            ) as mock_run:
                remote_file = MagicMock()
                result = c.download(remote_file)
                assert result is mock_local
                mock_run.assert_called_once()

    def test_get_local_hierarchy_is_sync(self, tmp_path):
        db = tmp_path / "test.db"
        client = PySUS(db_path=db)
        with client as c:
            result = c.get_local_hierarchy()
            assert isinstance(result, dict)

    def test_get_completed_remote_paths_is_sync(self, tmp_path):
        db = tmp_path / "test.db"
        client = PySUS(db_path=db)
        with client as c:
            result = c.get_completed_remote_paths()
            assert isinstance(result, set)

    def test_read_parquet_is_sync(self, tmp_path):
        from pysus.api.errors import ValidationError

        db = tmp_path / "test.db"
        client = PySUS(db_path=db)
        with client as c:
            with pytest.raises(ValidationError, match="No paths"):
                c.read_parquet([])

    def test_run_async_uses_persistent_loop(self, tmp_path):
        db = tmp_path / "test.db"
        with PySUS(db_path=db) as c:

            async def coro():
                return "ok"

            result = c._run_async(coro())
            assert result == "ok"

    def test_run_async_fallback_without_loop(self, tmp_path):
        db = tmp_path / "test.db"
        client = PySUS(db_path=db)
        assert client._loop is None

        async def coro():
            return "fallback"

        result = client._run_async(coro())
        assert result == "fallback"

    def test_context_manager_is_reusable(self, tmp_path):
        db = tmp_path / "test.db"
        with patch("pysus.api.client.DuckLake") as mock_dl:
            mock_dl.return_value.connect = AsyncMock()
            mock_dl.return_value.close = AsyncMock()

            client = PySUS(db_path=db)
            with client as c:
                assert c._sync is True
            assert client._sync is False

            with client as c:
                assert c._sync is True
            assert client._sync is False

    def test_async_context_still_works(self, tmp_path):
        """async with still works as before."""
        db = tmp_path / "test.db"

        async def _test():
            with patch("pysus.api.client.DuckLake") as mock_dl:
                mock_dl.return_value.connect = AsyncMock()
                mock_dl.return_value.close = AsyncMock()
                client = PySUS(db_path=db)
                async with client as c:
                    assert c._sync is False
                    assert c._ducklake is not None
                mock_dl.return_value.close.assert_awaited()

        asyncio.run(_test())

    def test_sync_sets_persistent_loop(self, tmp_path):
        db = tmp_path / "test.db"
        client = PySUS(db_path=db)
        with client as c:
            assert c._loop is not None
            assert not c._loop.is_closed()
        assert client._loop is None
