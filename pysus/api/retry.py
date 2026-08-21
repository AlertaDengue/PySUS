"""Centralized retry decorator with exponential backoff.

Usage::

    from pysus.api.retry import retry

    @retry(max_attempts=3, backoff_base=1.0)
    async def fetch(url):
        ...
"""

from __future__ import annotations

import asyncio
import functools
import logging
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger("pysus.retry")


def retry(
    max_attempts: int = 3,
    backoff_base: float = 1.0,
    backoff_max: float = 30.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> Callable:
    """Decorator that retries a function on transient failures.

    Parameters
    ----------
    max_attempts : int
        Maximum number of attempts (including the first).
    backoff_base : float
        Base delay in seconds (doubled each retry).
    backoff_max : float
        Maximum delay in seconds.
    exceptions : tuple
        Exception types to catch and retry on.

    Returns
    -------
    Callable
        Decorated function.

    Examples
    --------
    >>> @retry(max_attempts=3, exceptions=(ConnectionError,))
    ... async def fetch(url):
    ...     ...
    """

    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):
            return _async_retry(
                func, max_attempts, backoff_base, backoff_max, exceptions
            )
        return _sync_retry(
            func, max_attempts, backoff_base, backoff_max, exceptions
        )

    return decorator


def _sync_retry(
    func: Callable,
    max_attempts: int,
    backoff_base: float,
    backoff_max: float,
    exceptions: tuple[type[BaseException], ...],
) -> Callable:
    """Wrap a synchronous function with retry logic."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        last_exc: BaseException | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                return func(*args, **kwargs)
            except exceptions as exc:
                last_exc = exc
                if attempt < max_attempts:
                    delay = min(
                        backoff_base * (2 ** (attempt - 1)), backoff_max
                    )
                    logger.warning(
                        "Retrying %s (attempt %d/%d) after %.1fs: %s",
                        func.__qualname__,
                        attempt + 1,
                        max_attempts,
                        delay,
                        exc,
                    )
                    time.sleep(delay)
        raise last_exc  # type: ignore[misc]

    return wrapper


def _async_retry(
    func: Callable,
    max_attempts: int,
    backoff_base: float,
    backoff_max: float,
    exceptions: tuple[type[BaseException], ...],
) -> Callable:
    """Wrap an async function with retry logic."""

    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        last_exc: BaseException | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                return await func(*args, **kwargs)
            except exceptions as exc:
                last_exc = exc
                if attempt < max_attempts:
                    delay = min(
                        backoff_base * (2 ** (attempt - 1)), backoff_max
                    )
                    logger.warning(
                        "Retrying %s (attempt %d/%d) after %.1fs: %s",
                        func.__qualname__,
                        attempt + 1,
                        max_attempts,
                        delay,
                        exc,
                    )
                    await asyncio.sleep(delay)
        raise last_exc  # type: ignore[misc]

    return wrapper
