"""Parallel download utilities for PySUS.

Provides ``download_many()`` for concurrent downloads with progress
tracking.

Usage::

    from pysus.api.concurrent import download_many

    results = await download_many(files, max_workers=4)
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger("pysus.concurrent")


async def download_many(
    files: list[Any],
    download_fn: Callable,
    max_workers: int = 4,
    callback: Callable | None = None,
) -> list[Any]:
    """Download multiple files concurrently.

    Parameters
    ----------
    files : list
        List of file objects to download.
    download_fn : Callable
        Async function that downloads a single file.
        Signature: ``async def download_fn(file, callback) -> result``.
    max_workers : int
        Maximum concurrent downloads.
    callback : Callable, optional
        Progress callback ``(downloaded, total) -> None``.
        Called with aggregate progress across all files.

    Returns
    -------
    list
        Results from each download (same order as input files).
    """
    semaphore = asyncio.Semaphore(max_workers)
    total_files = len(files)
    completed = 0
    results: list[Any] = [None] * total_files

    async def _download_one(idx: int, file: Any) -> None:
        nonlocal completed
        async with semaphore:
            try:
                result = await download_fn(file, None)
                results[idx] = result
            except (OSError, ValueError, RuntimeError) as exc:
                logger.error("Failed to download %s: %s", file, exc)
                results[idx] = exc
            finally:
                completed += 1
                if callback:
                    callback(completed, total_files)

    tasks = [_download_one(i, f) for i, f in enumerate(files)]
    await asyncio.gather(*tasks)

    return results
