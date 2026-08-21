"""Tests for pysus.api.retry — centralized retry decorator."""

import time
from unittest.mock import MagicMock

import pytest
from pysus.api.retry import retry


class TestSyncRetry:
    def test_succeeds_first_attempt(self):
        mock = MagicMock(return_value="ok")

        @retry(max_attempts=3, backoff_base=0.01)
        def func():
            return mock()

        result = func()
        assert result == "ok"
        assert mock.call_count == 1

    def test_succeeds_after_retries(self):
        mock = MagicMock(
            side_effect=[ValueError("fail"), ValueError("fail"), "ok"]
        )

        @retry(max_attempts=3, backoff_base=0.01, exceptions=(ValueError,))
        def func():
            return mock()

        result = func()
        assert result == "ok"
        assert mock.call_count == 3

    def test_fails_after_max_attempts(self):
        mock = MagicMock(side_effect=ValueError("always fail"))

        @retry(max_attempts=3, backoff_base=0.01, exceptions=(ValueError,))
        def func():
            return mock()

        with pytest.raises(ValueError, match="always fail"):
            func()
        assert mock.call_count == 3

    def test_does_not_retry_on_unhandled_exception(self):
        mock = MagicMock(side_effect=TypeError("not caught"))

        @retry(max_attempts=3, backoff_base=0.01, exceptions=(ValueError,))
        def func():
            return mock()

        with pytest.raises(TypeError):
            func()
        assert mock.call_count == 1

    def test_backoff_timing(self):
        mock = MagicMock(side_effect=[ValueError, ValueError, "ok"])

        @retry(max_attempts=3, backoff_base=0.1, exceptions=(ValueError,))
        def func():
            return mock()

        start = time.monotonic()
        func()
        elapsed = time.monotonic() - start
        # Should have waited ~0.1 + ~0.2 = ~0.3s
        assert elapsed >= 0.2


class TestAsyncRetry:
    @pytest.mark.asyncio
    async def test_succeeds_first_attempt(self):
        mock = MagicMock(return_value="ok")

        @retry(max_attempts=3, backoff_base=0.01)
        async def func():
            return mock()

        result = await func()
        assert result == "ok"
        assert mock.call_count == 1

    @pytest.mark.asyncio
    async def test_succeeds_after_retries(self):
        mock = MagicMock(
            side_effect=[ValueError("fail"), ValueError("fail"), "ok"]
        )

        @retry(max_attempts=3, backoff_base=0.01, exceptions=(ValueError,))
        async def func():
            return mock()

        result = await func()
        assert result == "ok"
        assert mock.call_count == 3

    @pytest.mark.asyncio
    async def test_fails_after_max_attempts(self):
        mock = MagicMock(side_effect=ValueError("always fail"))

        @retry(max_attempts=3, backoff_base=0.01, exceptions=(ValueError,))
        async def func():
            return mock()

        with pytest.raises(ValueError, match="always fail"):
            await func()
        assert mock.call_count == 3
