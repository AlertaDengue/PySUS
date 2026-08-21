"""Tests for pysus.api.concurrent — parallel downloads."""

import asyncio

import pytest
from pysus.api.concurrent import download_many


class TestDownloadMany:
    @pytest.mark.asyncio
    async def test_basic_download(self):
        async def download_fn(file, cb):
            return file * 2

        results = await download_many([1, 2, 3], download_fn, max_workers=2)
        assert results == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_respects_max_workers(self):
        active = 0
        max_seen = 0

        async def download_fn(file, cb):
            nonlocal active, max_seen
            active += 1
            max_seen = max(max_seen, active)
            await asyncio.sleep(0.05)
            active -= 1
            return file

        await download_many(list(range(10)), download_fn, max_workers=3)
        assert max_seen <= 3

    @pytest.mark.asyncio
    async def test_callback_called(self):
        calls = []

        def cb(downloaded, total):
            calls.append((downloaded, total))

        async def download_fn(file, _cb):
            return file

        await download_many([1, 2, 3], download_fn, max_workers=2, callback=cb)
        assert len(calls) == 3
        assert calls[-1] == (3, 3)

    @pytest.mark.asyncio
    async def test_exception_captured(self):
        async def download_fn(file, cb):
            if file == 2:
                raise ValueError("fail")
            return file

        results = await download_many([1, 2, 3], download_fn, max_workers=2)
        assert results[0] == 1
        assert isinstance(results[1], ValueError)
        assert results[2] == 3

    @pytest.mark.asyncio
    async def test_empty_list(self):
        async def download_fn(file, cb):
            return file

        results = await download_many([], download_fn)
        assert results == []

    @pytest.mark.asyncio
    async def test_preserves_order(self):
        async def download_fn(file, cb):
            await asyncio.sleep(0.01 * (5 - file))
            return file

        results = await download_many(
            [5, 4, 3, 2, 1], download_fn, max_workers=5
        )
        assert results == [5, 4, 3, 2, 1]
