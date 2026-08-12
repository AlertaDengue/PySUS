"""Sync engine: the end-to-end workflow across FTP, DadosGov and S3.

Pipeline per dataset:

1. inventory — snapshot every file visible on each client
   (:class:`~pysus.management.inventory.Inventory`);
2. compare — group artifacts into logical files
   (:class:`~pysus.management.compare.Comparator`);
3. resolve — pick the download source following the fixed priority
   S3 → FTP → DadosGov (token required only for DadosGov-only files);
4. load — download, convert to parquet, upload to S3, and persist the
   metadata in the DuckLake catalog (:class:`~pysus.management.catalog
   .CatalogWriter`).

Single-writer assumption: the DuckLake catalog duckdbs are uploaded as
whole files on close; concurrent sync runs would clobber each other.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime
from logging import error
from typing import TYPE_CHECKING, Any, cast

from pysus.api.dadosgov.models import File as APIFile
from pysus.api.ducklake.functional import upload_s3
from pysus.api.errors import AuthenticationError, ConnectionError
from pysus.api.extensions import Parquet
from pysus.api.ftp.models import File as FTPFile
from pysus.api.models import BaseRemoteFile

from .catalog import CatalogWriter, sha256_of
from .compare import Comparator
from .inventory import Inventory
from .records import (
    DOWNLOAD_PRIORITY,
    FileComparison,
    FileRecord,
    SyncOutcome,
    SyncReport,
    compose_s3_key,
)

if TYPE_CHECKING:  # pragma: no cover
    from pysus.api.client import PySUS
    from pysus.api.ducklake.client import DuckLake

_RETRYABLE = (ConnectionResetError, ConnectionRefusedError, TimeoutError)


class _DatasetStub:
    """Registry entry for a per-dataset adapter without a full DuckDataset.

    ``DuckLake.close`` iterates ``_datasets`` and calls ``ds.close``;
    the stub delegates to its adapter so the catalog is uploaded on close.
    """

    def __init__(self, name: str, adapter):
        self.name = name
        self.adapter = adapter

    async def close(self, update_catalog: bool | None = None) -> None:
        await self.adapter.close(update=bool(update_catalog))


class SyncEngine:
    """Orchestrates inventory → compare → download → parquet → catalog."""

    pysus: PySUS | None
    _ducklake: DuckLake | None

    def __init__(
        self,
        access_key: str | None = None,
        secret_key: str | None = None,
        dadosgov_token: str | None = None,
        pysus: PySUS | None = None,
    ):
        self.access_key = access_key
        self.secret_key = secret_key
        self.dadosgov_token = dadosgov_token
        self.pysus = pysus
        self._ducklake = None
        self._changed_catalog = False

    def _require_pysus(self) -> PySUS:
        if self.pysus is None:
            raise ConnectionError("PySUS orchestrator is not connected")
        return self.pysus

    def _require_ducklake(self) -> DuckLake:
        if self._ducklake is None:
            raise ConnectionError("DuckLake is not connected")
        return self._ducklake

    @property
    def inventory(self) -> Inventory:
        return Inventory(self._require_pysus())

    @property
    def comparator(self) -> Comparator:
        return Comparator()

    @property
    def writer(self) -> CatalogWriter:
        return CatalogWriter(self._require_ducklake())

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    async def __aenter__(self) -> SyncEngine:
        if self.pysus is None:
            from pysus.api.client import PySUS

            self.pysus = PySUS()
        await self.pysus.__aenter__()

        self._ducklake = await self.pysus.get_ducklake()
        if self.access_key and self.secret_key:
            await self._ducklake.login(
                access_key=self.access_key,
                secret_key=self.secret_key,
            )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if not exc_type and self._ducklake:
                await self._ducklake.close(update_catalog=self._changed_catalog)
        finally:
            if self.pysus is not None:
                await self.pysus.__aexit__(exc_type, exc_val, exc_tb)

    # ------------------------------------------------------------------
    # single-file upload (used by CatalogManager)
    # ------------------------------------------------------------------
    def s3_key_for(self, file: BaseRemoteFile) -> str:
        """Return the hierarchical S3 key for *file*'s parquet artifact."""
        group = getattr(file, "group", None)
        return compose_s3_key(
            origin=file.client.name,
            dataset=file.dataset.name,
            name=file.basename,
            group=getattr(group, "name", None) if group else None,
            year=file.year,
            month=file.month,
            state=file.state,
        )

    async def upload_file(
        self,
        file: FTPFile | APIFile,
        callback: Callable[[int, int], None] | None = None,
        force: bool = False,
    ) -> bool:
        """Download *file*, convert to parquet, upload and catalog it.

        Writes metadata into the correct catalogs: the dataset registry
        row into ``catalog.duckdb``, the file/group rows into the
        per-dataset ``catalog_<name>.duckdb``, and the column definitions
        into ``catalog_columns.duckdb``.

        Returns True if the file was processed; False when the catalog
        already holds an equally recent artifact (skip).
        """
        if self._ducklake is None:
            raise ConnectionError("DuckLake is not connected")

        s3_key = self.s3_key_for(file)
        writer = self.writer

        dataset_adapter = self._dataset_adapter(file)
        central_adapter = self._ducklake._catalog_adap
        columns_adapter = self._ducklake._columns_adap

        await central_adapter.connect()
        await columns_adapter.connect()
        await dataset_adapter.connect()

        central_conn = central_adapter._engine.raw_connection()
        dataset_conn = dataset_adapter._engine.raw_connection()
        columns_conn = columns_adapter._engine.raw_connection()

        connections = (central_conn, dataset_conn, columns_conn)
        try:
            with central_conn, dataset_conn, columns_conn:
                central_cursor = central_conn.cursor()
                dataset_cursor = dataset_conn.cursor()
                columns_cursor = columns_conn.cursor()

                writer._ensure_management_columns(dataset_cursor)

                existing = writer.get_file(dataset_cursor, s3_key)
                if existing and not force:
                    _, origin_modified = existing
                    if self._is_current(file, origin_modified):
                        return False

                dataset_id = writer.ensure_dataset(
                    central_cursor,
                    file.dataset.name,
                    file.dataset.long_name,
                    getattr(file.dataset, "description", None),
                )

                group = getattr(file, "group", None)
                group_name = (
                    getattr(group, "name", None) if group is not None else None
                )
                group_name = str(group_name) if group_name else None
                group_id = writer.ensure_group(
                    dataset_cursor,
                    dataset_id,
                    group_name,
                    getattr(group, "long_name", None) if group else None,
                    getattr(group, "description", None) if group else None,
                )

                parquet_file = await self._download_with_retry(file, callback)
                await upload_s3(
                    local_path=parquet_file.path,
                    remote_path=s3_key,
                    access_key=str(self.access_key),
                    secret_key=str(self.secret_key),
                    callback=callback,
                )

                digest = sha256_of(parquet_file.path)
                writer.upsert_file(
                    dataset_cursor,
                    dataset_id=dataset_id,
                    group_id=group_id,
                    path=s3_key,
                    size=parquet_file.size,
                    rows=parquet_file.rows,
                    modified=datetime.now(),
                    origin_modified=self._safe_modify(file),
                    origin_size=self._safe_size(file),
                    origin_path=str(file.path),
                    year=file.year,
                    month=file.month,
                    state=file.state,
                    origin=file.client.name.lower(),
                    format="parquet",
                    sha256=digest,
                    file_type="PARQUET",
                )

                inserted = writer.get_file(dataset_cursor, s3_key)
                assert inserted is not None
                file_id, _ = inserted
                writer.link_columns(
                    dataset_cursor,
                    columns_cursor,
                    file_id,
                    parquet_file.schema,
                    dataset_id,
                )

                central_conn.commit()
                dataset_conn.commit()
                columns_conn.commit()
                dataset_cursor.execute("CHECKPOINT")
                columns_cursor.execute("CHECKPOINT")

                central_adapter._local_dirty = True
                dataset_adapter._local_dirty = True
                columns_adapter._local_dirty = True
                self._changed_catalog = True
                return True
        except BaseException as exc:  # noqa
            # the connection context managers roll back on exit
            for conn in connections:
                try:
                    conn.close()
                except Exception:  # noqa
                    pass
            raise exc

    def _dataset_adapter(self, file: BaseRemoteFile):
        """Return (and register) the per-dataset adapter for *file*."""
        ducklake = self._require_ducklake()
        dataset_name = file.dataset.name.lower()
        for ds in ducklake._datasets:
            if getattr(ds, "name", "").lower() == dataset_name:
                return ds.adapter

        from pysus.api.ducklake.catalog.adapters import DatasetAdapter

        adapter = DatasetAdapter(
            name=dataset_name,
            dataset_id=0,
            credentials=ducklake.credentials,
            update_on_close=ducklake.update_on_close,
        )
        ducklake._datasets.append(
            cast(Any, _DatasetStub(dataset_name, adapter))
        )
        return adapter

    async def _download_with_retry(
        self,
        file: FTPFile | APIFile,
        callback: Callable[[int, int], None] | None = None,
        max_retries: int = 3,
    ) -> Parquet:
        last_error: Exception | None = None
        token = (
            self.dadosgov_token
            if file.client.name.lower() == "dadosgov"
            else None
        )
        for attempt in range(max_retries):
            try:
                return await self._require_pysus().download_to_parquet(
                    file=file,
                    token=token,
                    callback=callback,
                )
            except _RETRYABLE as exc:
                last_error = exc
                wait_time = 2**attempt
                error(
                    f"Download attempt {attempt + 1}/{max_retries} failed "
                    f"for {file.basename}: {exc}. Retrying in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)

        raise RuntimeError(
            f"Failed to download {file.basename} after {max_retries} "
            f"attempts: {last_error}"
        ) from last_error

    @staticmethod
    def _is_current(
        file: BaseRemoteFile,
        origin_modified: datetime | None,
    ) -> bool:
        if origin_modified is None:
            return False
        try:
            file_mod = file.modify
        except ValueError:
            return False
        return str(file_mod) <= str(origin_modified)

    @staticmethod
    def _safe_modify(file: BaseRemoteFile) -> datetime | None:
        try:
            return file.modify
        except ValueError:
            return None

    @staticmethod
    def _safe_size(file: BaseRemoteFile) -> int:
        try:
            return file.size
        except (ValueError, AttributeError):
            return 0

    # ------------------------------------------------------------------
    # full sync run
    # ------------------------------------------------------------------
    async def run(
        self,
        datasets: list[str] | None = None,
        force: bool = False,
        callback: Callable[[int, int], None] | None = None,
        save_snapshots: bool = True,
        checkpoint_every: int | None = None,
        on_outcome: Callable[[SyncOutcome], None] | None = None,
    ) -> SyncReport:
        """Run the full pipeline and return a :class:`SyncReport`.

        Files already on S3 (ducklake artifacts) are skipped; FTP is
        preferred over DadosGov, which requires ``dadosgov_token``.
        Files whose non-S3 artifact is newer than the S3 copy are
        reprocessed (most-updated policy).

        ``checkpoint_every`` uploads the modified catalogs to S3 every N
        successful uploads, making long runs resumable (files already
        cataloged are skipped on the next run). ``on_outcome`` is called
        once per processed logical file (e.g. for progress logging).
        """
        report = SyncReport(dataset=",".join(datasets) if datasets else None)

        records: dict[str, list[FileRecord]] = {
            "ducklake": await self.inventory.collect("ducklake", datasets),
            "ftp": await self.inventory.collect("ftp", datasets),
        }
        records["dadosgov"] = []
        if self.dadosgov_token:
            records["dadosgov"] = await self.inventory.collect(
                "dadosgov", datasets, dadosgov_token=self.dadosgov_token
            )

        comparisons = self.comparator.compare(
            records["ducklake"] + records["ftp"] + records["dadosgov"]
        )

        uploaded_since_checkpoint = 0
        for comparison in comparisons:
            outcome = await self._process_comparison(
                comparison, force=force, callback=callback
            )
            report.outcomes.append(outcome)
            if on_outcome:
                on_outcome(outcome)

            if (
                outcome.status == "uploaded"
                and checkpoint_every
                and self._changed_catalog
            ):
                uploaded_since_checkpoint += 1
                if uploaded_since_checkpoint >= checkpoint_every:
                    await self._checkpoint()
                    uploaded_since_checkpoint = 0

        if self._changed_catalog and checkpoint_every is not None:
            await self._checkpoint()

        if save_snapshots:
            for origin, items in records.items():
                self.inventory.save_snapshot(origin, items)

        return report

    async def _checkpoint(self) -> None:
        """Upload all dirty catalogs to S3 and reconnect the adapters."""
        ducklake = self._require_ducklake()
        for ds in ducklake._datasets:
            await ds.close(update_catalog=True)
        await ducklake._catalog_adap.close(update=True)
        await ducklake._columns_adap.close(update=True)
        await ducklake._catalog_adap.connect()
        await ducklake._columns_adap.connect()
        self._changed_catalog = False

    async def _process_comparison(
        self,
        comparison: FileComparison,
        force: bool = False,
        callback: Callable[[int, int], None] | None = None,
    ) -> SyncOutcome:
        key = comparison.key
        label = (
            f"{key.dataset}/{key.group or '-'}/"
            f"{key.year or '-'}/{key.month or '-'}/{key.stem}"
        )

        if comparison.is_on_s3:
            if not force and not self._s3_is_stale(comparison):
                return SyncOutcome(key=key, origin="ducklake", status="skipped")
            return await self._reprocess(comparison, callback, force, label)

        return await self._reprocess(comparison, callback, force, label)

    @staticmethod
    def _s3_is_stale(comparison: FileComparison) -> bool:
        """True when any non-S3 artifact is newer than the S3 copy."""
        s3_record = comparison._pick("ducklake")
        if s3_record is None:
            return False

        s3_source_modified = s3_record.source_modified or s3_record.modified
        if s3_source_modified is None:
            return False

        for record in comparison.records:
            if record.origin == "ducklake":
                continue
            modified = record.modified
            if modified and modified > s3_source_modified:
                return True
        return False

    async def _reprocess(
        self,
        comparison: FileComparison,
        callback: Callable[[int, int], None] | None,
        force: bool,
        label: str,
    ) -> SyncOutcome:
        key = comparison.key
        for origin in DOWNLOAD_PRIORITY[1:]:  # ducklake already checked
            record = comparison._pick(origin)
            if record is None or record.file is None:
                continue
            if origin == "dadosgov" and not self.dadosgov_token:
                return SyncOutcome(
                    key=key,
                    origin="dadosgov",
                    status="needs_token",
                    detail=f"only on DadosGov: {label}",
                )
            try:
                processed = await self.upload_file(
                    record.file,
                    callback=callback,
                    force=force,
                )
                if processed:
                    return SyncOutcome(
                        key=key,
                        origin=origin,
                        status="uploaded",
                        detail=label,
                    )
                return SyncOutcome(
                    key=key,
                    origin=origin,
                    status="skipped",
                    detail=f"already current: {label}",
                )
            except AuthenticationError as exc:
                return SyncOutcome(
                    key=key,
                    origin=origin,
                    status="needs_token",
                    detail=str(exc),
                )
            except Exception as exc:  # noqa
                return SyncOutcome(
                    key=key,
                    origin=origin,
                    status="failed",
                    detail=f"{label}: {exc}",
                )

        return SyncOutcome(
            key=key,
            origin="unknown",
            status="failed",
            detail=f"no downloadable artifact: {label}",
        )
