"""Catalog management entry point.

``CatalogManager`` is a thin facade over
:class:`~pysus.management.sync.SyncEngine`: it downloads a remote file,
converts it to parquet, uploads it to S3 and upserts the metadata into the
DuckLake catalog. The full cross-client workflow (inventory → compare →
sync) lives in :mod:`pysus.management.sync`.
"""

from __future__ import annotations

import os
from collections.abc import Callable

from pysus.api.dadosgov.models import File as APIFile
from pysus.api.ftp.models import File as FTPFile

from .sync import SyncEngine


class CatalogManager:
    """Upload + catalog single files into the PySUS S3 bucket."""

    def __init__(
        self,
        access_key: str | None = None,
        secret_key: str | None = None,
        dadosgov_token: str | None = None,
    ):
        self.engine = SyncEngine(
            access_key=access_key or os.getenv("ACCESS_KEY"),
            secret_key=secret_key or os.getenv("SECRET_KEY"),
            dadosgov_token=dadosgov_token or os.getenv("DADOSGOV_TOKEN"),
        )
        if not self.engine.access_key or not self.engine.secret_key:
            raise ValueError("s3 credentials are needed")

    async def __aenter__(self) -> CatalogManager:
        await self.engine.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.engine.__aexit__(exc_type, exc_val, exc_tb)

    async def upload(
        self,
        file: FTPFile | APIFile,
        callback: Callable[[int, int], None] | None = None,
        force: bool = False,
    ) -> bool:
        """Download, convert, upload and catalog a single remote file.

        Returns True if the file was (re)processed, False if the catalog
        already holds an equally recent artifact.
        """
        return await self.engine.upload_file(
            file, callback=callback, force=force
        )
