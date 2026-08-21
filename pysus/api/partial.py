"""Partial download recovery with HTTP Range resume.

Provides utilities for detecting incomplete downloads and resuming
them using HTTP Range headers.

Usage::

    from pysus.api.partial import PartialDownload

    partial = PartialDownload(dest_path)
    if partial.exists():
        await partial.resume(url, client, callback)
    else:
        await partial.start(url, client, callback)
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

import httpx

logger = logging.getLogger("pysus.partial")


class PartialDownload:
    """Manages partial file downloads with resume support.

    Parameters
    ----------
    dest_path : Path
        Final destination path for the downloaded file.
    """

    def __init__(self, dest_path: Path) -> None:
        self.dest_path = dest_path
        self.partial_path = dest_path.with_suffix(dest_path.suffix + ".partial")

    def exists(self) -> bool:
        """Check if a partial download exists."""
        return self.partial_path.exists()

    def size(self) -> int:
        """Return the size of the partial file in bytes."""
        if self.exists():
            return self.partial_path.stat().st_size
        return 0

    async def start(
        self,
        url: str,
        client: httpx.AsyncClient,
        callback: Callable | None = None,
    ) -> Path:
        """Start a fresh download.

        Parameters
        ----------
        url : str
            URL to download from.
        client : httpx.AsyncClient
            HTTP client to use.
        callback : callable, optional
            Progress callback ``(downloaded, total) -> None``.

        Returns
        -------
        Path
            Path to the completed file.
        """
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            total = int(response.headers.get("content-length", 0))

            self.partial_path.parent.mkdir(parents=True, exist_ok=True)
            downloaded = 0

            with open(self.partial_path, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if callback:
                        callback(downloaded, total)

        self.partial_path.rename(self.dest_path)
        return self.dest_path

    async def resume(
        self,
        url: str,
        client: httpx.AsyncClient,
        callback: Callable | None = None,
    ) -> Path:
        """Resume a partial download using HTTP Range.

        Parameters
        ----------
        url : str
            URL to download from.
        client : httpx.AsyncClient
            HTTP client to use.
        callback : callable, optional
            Progress callback ``(downloaded, total) -> None``.

        Returns
        -------
        Path
            Path to the completed file.

        Notes
        -----
        If the server does not support Range requests, falls back to
        a fresh download.
        """
        existing_size = self.size()
        if existing_size == 0:
            return await self.start(url, client, callback)

        try:
            headers = {"Range": f"bytes={existing_size}-"}
            async with client.stream("GET", url, headers=headers) as response:
                if response.status_code == 206:
                    total = int(
                        response.headers.get("content-range", "").split("/")[-1]
                    )
                    downloaded = existing_size

                    with open(self.partial_path, "ab") as f:
                        async for chunk in response.aiter_bytes(
                            chunk_size=65536
                        ):
                            f.write(chunk)
                            downloaded += len(chunk)
                            if callback:
                                callback(downloaded, total)

                    self.partial_path.rename(self.dest_path)
                    return self.dest_path
                else:
                    logger.info(
                        "Server returned %d, restarting download",
                        response.status_code,
                    )
                    return await self.start(url, client, callback)
        except httpx.HTTPError as exc:
            logger.warning("Range request failed: %s, restarting", exc)
            return await self.start(url, client, callback)
