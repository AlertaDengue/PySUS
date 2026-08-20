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
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import httpx
from pysus import CACHEPATH
from pysus.api.ducklake.functional import upload_s3
from pysus.api.errors import AuthenticationError, ConnectionError
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

_RETRYABLE = (
    ConnectionResetError,
    ConnectionRefusedError,
    TimeoutError,
    BrokenPipeError,
    OSError,
)


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
        self._acquire_sync_lock()
        return self

    def _acquire_sync_lock(self) -> None:
        """Guarantee a single sync process owns the catalogs at a time.

        DuckDB catalog files allow one writer process; concurrent syncs
        corrupt each other's state. The lock file carries the PID and is
        stolen only when that PID is no longer alive.
        """
        import os

        lock_path = Path(CACHEPATH) / "ducklake" / ".sync.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        if lock_path.exists():
            try:
                owner = int(lock_path.read_text().strip() or "0")
            except ValueError:
                owner = 0
            alive = owner > 0 and self._pid_alive(owner)
            if alive:
                raise ConnectionError(
                    "another sync process (PID "
                    f"{owner}) holds the catalog lock"
                )
        lock_path.write_text(str(os.getpid()))

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        import os

        if os.name == "nt":
            try:
                import ctypes

                windll = getattr(ctypes, "windll", None)
                if windll is None:
                    return False
                process_query_limited = 0x1000
                handle = windll.kernel32.OpenProcess(
                    process_query_limited, False, pid
                )
                if not handle:
                    return False
                windll.kernel32.CloseHandle(handle)
                return True
            except Exception:  # noqa
                return False

        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            # invalid/out-of-range pid on some platforms
            return False
        return True

    def _release_sync_lock(self) -> None:
        lock_path = Path(CACHEPATH) / "ducklake" / ".sync.lock"
        try:
            lock_path.unlink(missing_ok=True)
        except OSError:
            pass

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if not exc_type and self._ducklake:
                await self._ducklake.close(update_catalog=self._changed_catalog)
        finally:
            self._release_sync_lock()
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
        file: BaseRemoteFile,
        callback: Callable[[int, int], None] | None = None,
        force: bool = False,
        ftp_client: Any | None = None,
    ) -> bool:
        """Download *file*, convert to parquet, upload and catalog it.

        Writes metadata into the correct catalogs: the dataset registry
        row into ``catalog.duckdb``, the file/group rows into the
        per-dataset ``catalog_<name>.duckdb``, and the column definitions
        into ``catalog_columns.duckdb``.

        ``ftp_client`` (optional) is used instead of the file's client
        for the raw download, allowing a pool of FTP connections to be
        shared safely across parallel workers.

        Content-veto (no mismatch possible — byte-exact hashes):

        * if the source ``modified`` is not newer than the catalog's
          ``origin_modified`` → skip without downloading;
        * else the raw file is downloaded (cache bypassed) and hashed:
          same raw ``sha256`` as the stored ``source_sha256`` → only the
          origin metadata is touched, no conversion/upload;
        * else the parquet is converted and hashed: same parquet
          ``sha256`` as stored → only metadata is touched, no upload;
        * otherwise the artifact is uploaded and both hashes stored.

        Returns True if the file was (re)processed; False when the
        existing artifact is current or content-identical.
        """
        if self._ducklake is None:
            raise ConnectionError("DuckLake is not connected")

        s3_key = self.s3_key_for(file)
        writer = self.writer

        dataset_adapter = self._dataset_adapter(file)
        central_adapter = self._ducklake.catalog_adapter
        columns_adapter = self._ducklake.columns_adapter

        await central_adapter.connect()
        await columns_adapter.connect()
        await dataset_adapter.connect()

        central_conn = central_adapter.raw_connection()
        dataset_conn = dataset_adapter.raw_connection()
        columns_conn = columns_adapter.raw_connection()

        connections = (central_conn, dataset_conn, columns_conn)
        try:
            with central_conn, dataset_conn, columns_conn:
                central_cursor = central_conn.cursor()
                dataset_cursor = dataset_conn.cursor()
                columns_cursor = columns_conn.cursor()

                writer._ensure_management_columns(dataset_cursor)

                existing = writer.get_file_full(dataset_cursor, s3_key)
                if existing and not force:
                    _, origin_modified, _, _, _ = existing
                    if self._is_current(file, origin_modified):
                        return False

                raw_path = await self._download_raw_with_retry(
                    file, ftp_client=ftp_client
                )
                raw_digest = sha256_of(raw_path)

                if existing and not force:
                    file_id = existing[0]
                    stored_source = existing[4]
                    if stored_source and raw_digest == stored_source:
                        writer.touch_file(
                            dataset_cursor,
                            file_id,
                            self._safe_modify(file),
                            self._safe_size(file),
                        )
                        dataset_conn.commit()
                        dataset_cursor.execute("CHECKPOINT")
                        dataset_adapter.mark_dirty()
                        self._changed_catalog = True
                        self._cleanup_local(raw_path)
                        return False

                from pysus.api.extensions import ExtensionFactory

                local_file = await ExtensionFactory.instantiate(raw_path)
                if not hasattr(local_file, "to_parquet"):
                    raise RuntimeError(
                        f"{file.basename}: cannot convert to parquet"
                    )
                parquet_file = await local_file.to_parquet(
                    callback=callback,
                )
                parquet_digest = sha256_of(parquet_file.path)

                if existing and not force:
                    file_id = existing[0]
                    stored_sha = existing[3]
                    if stored_sha and parquet_digest == stored_sha:
                        writer.touch_file(
                            dataset_cursor,
                            file_id,
                            self._safe_modify(file),
                            self._safe_size(file),
                            source_sha256=raw_digest,
                        )
                        dataset_conn.commit()
                        dataset_cursor.execute("CHECKPOINT")
                        dataset_adapter.mark_dirty()
                        self._changed_catalog = True
                        self._cleanup_local(raw_path)
                        self._cleanup_local(parquet_file.path)
                        return False

                await upload_s3(
                    local_path=parquet_file.path,
                    remote_path=s3_key,
                    access_key=str(self.access_key),
                    secret_key=str(self.secret_key),
                    callback=callback,
                )

                payload = {
                    "s3_key": s3_key,
                    "size": parquet_file.path.stat().st_size,
                    "rows": parquet_file.rows,
                    "schema": parquet_file.schema,
                    "raw_digest": raw_digest,
                    "parquet_digest": parquet_digest,
                }
                self._catalog_rows(
                    central_cursor,
                    dataset_cursor,
                    columns_cursor,
                    file,
                    payload,
                )

                central_conn.commit()
                dataset_conn.commit()
                columns_conn.commit()
                dataset_cursor.execute("CHECKPOINT")
                columns_cursor.execute("CHECKPOINT")

                central_adapter.mark_dirty()
                dataset_adapter.mark_dirty()
                columns_adapter.mark_dirty()
                self._changed_catalog = True
                self._cleanup_local(raw_path)
                self._cleanup_local(parquet_file.path)
                return True
        except BaseException as exc:  # noqa
            # the connection context managers roll back on exit
            for conn in connections:
                try:
                    conn.close()
                except Exception:  # noqa
                    pass
            raise exc

    def _dataset_adapter_by_name(self, dataset_name: str):
        """Return (and register) the per-dataset adapter for *name*."""
        return self._require_ducklake().get_dataset_adapter(dataset_name)

    def _dataset_adapter(self, file: BaseRemoteFile):
        """Return (and register) the per-dataset adapter for *file*."""
        return self._dataset_adapter_by_name(file.dataset.name)

    async def _download_raw_with_retry(
        self,
        file: BaseRemoteFile,
        max_retries: int = 5,
        ftp_client: Any | None = None,
    ) -> Path:
        """Download the raw artifact bypassing the local cache.

        The PySUS local cache matches on size only, which could serve
        stale bytes for a same-size update — the content veto must hash
        exactly what the client serves now. ``ftp_client`` overrides the
        file's own FTP connection (pooled clients are not shared).
        """
        raw_dir = Path(CACHEPATH) / "management" / "tmp"
        raw_dir.mkdir(parents=True, exist_ok=True)
        output = raw_dir / f"{uuid4().hex[:8]}-{file.basename}"

        last_error: Exception | None = None
        for attempt in range(max_retries):
            try:
                await self._download_once(file, output, ftp_client)
                return output
            except _RETRYABLE as exc:
                last_error = exc
                wait_time = 2**attempt + (attempt * 2)
                error(
                    f"Download attempt {attempt + 1}/{max_retries} failed "
                    f"for {file.basename}: {exc}. Retrying in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)

        raise RuntimeError(
            f"Failed to download {file.basename} after {max_retries} "
            f"attempts: {last_error}"
        ) from last_error

    async def _download_once(
        self,
        file: BaseRemoteFile,
        output: Path,
        ftp_client: Any | None = None,
    ) -> None:
        """Perform one raw download to *output*."""
        from anyio import to_thread

        client = ftp_client if ftp_client is not None else file.client
        ftp = getattr(client, "ftp", None)
        if ftp_client is not None:
            # never fall back to the shared client: reconnect the pooled
            # session instead
            if ftp is None:
                await client.connect()
                ftp = getattr(client, "ftp", None)
            assert ftp is not None
            remote_path = str(file.path)

            def _retr():
                total = ftp.size(remote_path) or 0
                with open(output, "wb") as f:
                    ftp.retrbinary(
                        f"RETR {remote_path}", lambda chunk: f.write(chunk)
                    )
                return total

            try:
                await to_thread.run_sync(_retr)
                return
            except Exception:  # noqa
                try:
                    ftp.quit()
                except Exception:  # noqa
                    pass
                setattr(  # noqa: B010 — reset pooled FTP session
                    client, "_ftp", None
                )
                raise
        if ftp is not None:
            remote_path = str(file.path)

            def _direct_retr():
                with open(output, "wb") as f:
                    ftp.retrbinary(
                        f"RETR {remote_path}", lambda chunk: f.write(chunk)
                    )

            await to_thread.run_sync(_direct_retr)
            return
        await file._download(output=output)

    @staticmethod
    def _cleanup_local(path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass

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
        workers: int = 16,
        ftp_connections: int = 6,
        origins: tuple[str, ...] | None = None,
    ) -> SyncReport:
        """Run the full pipeline and return a :class:`SyncReport`.

        Files already on S3 (ducklake artifacts) are skipped unless
        ``force`` or the source size proves a change (trust-the-catalog
        policy); FTP is preferred over DadosGov, which requires
        ``dadosgov_token``.

        Missing files are ingested in parallel: ``workers`` asyncio tasks
        download (via a pool of ``ftp_connections`` FTP clients), convert
        and upload concurrently; catalog writes stay serialized and
        checkpoints only run when all workers are quiescent.

        ``checkpoint_every`` uploads the modified catalogs to S3 every N
        successful uploads, making long runs resumable. ``on_outcome`` is
        called once per processed logical file.
        """
        report = SyncReport(dataset=",".join(datasets) if datasets else None)
        active_origins = origins or ("ducklake", "ftp", "dadosgov", "saude")

        async def collect_with_retry(origin: str, datasets=None, **kwargs):
            for attempt in range(3):
                try:
                    return await self.inventory.collect(origin, **kwargs)
                except (*_RETRYABLE, httpx.HTTPError):
                    if attempt == 2:
                        raise
                    await asyncio.sleep(2**attempt)

        records: dict[str, list[FileRecord]] = {
            k: [] for k in ("ducklake", "ftp", "dadosgov", "saude")
        }
        if "ducklake" in active_origins:
            records["ducklake"] = await collect_with_retry("ducklake", datasets)
        if "ftp" in active_origins:
            records["ftp"] = await collect_with_retry("ftp", datasets)
        if "dadosgov" in active_origins:
            records["dadosgov"] = []
            if self.dadosgov_token:
                records["dadosgov"] = await collect_with_retry(
                    "dadosgov", datasets, dadosgov_token=self.dadosgov_token
                )
        if "saude" in active_origins:
            records["saude"] = await collect_with_retry("saude", datasets)

        comparisons = self.comparator.compare(sum(records.values(), []))

        # Pre-connect every adapter involved so concurrent workers never
        # race the initial catalog download.
        await self._preconnect_adapters(records)

        # Deduplicate: when the same logical file has artifacts from more
        # than one origin on S3 (legacy ETL), keep only the most updated
        # one — the bucket must never hold duplicate copies.
        await self._dedupe_s3_artifacts(records["ducklake"])

        # Repair rows cataloged by the old SIA formatter, which merged
        # part suffixes into the month (e.g. BIRJ2504_2 -> month 42).
        await self._fix_misparsed_metadata(records["ducklake"])

        parallel: list[tuple[FileComparison, FileRecord]] = []
        for comparison in comparisons:
            if comparison.is_on_s3 and not (
                force or self._s3_is_stale(comparison)
            ):
                outcome = SyncOutcome(
                    key=comparison.key,
                    origin="ducklake",
                    status="skipped",
                )
                report.outcomes.append(outcome)
                if on_outcome:
                    on_outcome(outcome)
                continue
            record = self._pick_source(comparison)
            if record is None:
                outcome = await self._process_comparison(
                    comparison, force=force, callback=callback
                )
                report.outcomes.append(outcome)
                if on_outcome:
                    on_outcome(outcome)
                continue
            parallel.append((comparison, record))

        ftp_items = [(c, r) for c, r in parallel if r.origin == "ftp"]
        gov_items = [(c, r) for c, r in parallel if r.origin != "ftp"]

        ftp_pool: list[Any] = []
        if ftp_items:
            from pysus.api.ftp.client import FTP

            for _ in range(ftp_connections):
                client = FTP()
                await client.connect()
                ftp_pool.append(client)

        raw_queue: asyncio.Queue = asyncio.Queue(maxsize=workers * 2)
        write_queue: asyncio.Queue = asyncio.Queue(maxsize=workers * 2)

        async def ftp_downloader(
            client: Any, items: list[tuple[FileComparison, FileRecord]]
        ) -> None:
            for comparison, record in items:
                try:
                    raw = await self._download_raw_with_retry(
                        record.file, ftp_client=client
                    )
                    await raw_queue.put((comparison, record, raw, None))
                except Exception as exc:  # noqa
                    await raw_queue.put((comparison, record, None, str(exc)))
            await raw_queue.put(None)

        async def raw_processor() -> None:
            while True:
                entry = await raw_queue.get()
                try:
                    if entry is None:
                        return
                    comparison, record, raw, err = entry
                    if err is not None:
                        await write_queue.put((comparison, record, None, err))
                        continue
                    payload = await self._convert_and_upload(
                        record.file, raw, callback=callback
                    )
                    await write_queue.put((comparison, record, payload, None))
                except Exception as exc:  # noqa
                    comparison, record, _, _ = entry
                    await write_queue.put((comparison, record, None, str(exc)))
                finally:
                    raw_queue.task_done()

        async def gov_worker() -> None:
            while gov_items:
                comparison, record = gov_items.pop()
                try:
                    raw = await self._download_raw_with_retry(record.file)
                    payload = await self._convert_and_upload(
                        record.file, raw, callback=callback
                    )
                    await write_queue.put((comparison, record, payload, None))
                except Exception as exc:  # noqa
                    await write_queue.put((comparison, record, None, str(exc)))

        async def catalog_writer() -> None:
            """Serial consumer: catalog rows + outcomes + checkpoints."""
            uploaded_since_checkpoint = 0

            ducklake = self._require_ducklake()
            central_adapter = ducklake.catalog_adapter
            columns_adapter = ducklake.columns_adapter
            await central_adapter.connect()
            await columns_adapter.connect()
            dataset_adapters: dict[str, Any] = {}

            done_writers = 0
            while True:
                entry = await write_queue.get()
                try:
                    if entry is None:
                        done_writers += 1
                        if done_writers >= writers_total:
                            break
                        continue

                    comparison, record, payload, err = entry
                    if err is not None:
                        outcome = SyncOutcome(
                            key=comparison.key,
                            origin=record.origin,
                            status="failed",
                            detail=f"{self._label(comparison)}: {err}",
                        )
                    else:
                        try:
                            adapter = dataset_adapters.get(
                                record.dataset.lower()
                            )
                            if adapter is None:
                                adapter = self._dataset_adapter_by_name(
                                    record.dataset
                                )
                            dataset_adapters[record.dataset.lower()] = adapter
                            self._catalog_write_entry(
                                adapter,
                                central_adapter,
                                columns_adapter,
                                record.file,
                                payload,
                            )
                            adapter.mark_dirty()
                            central_adapter.mark_dirty()
                            columns_adapter.mark_dirty()
                            self._changed_catalog = True
                            outcome = SyncOutcome(
                                key=comparison.key,
                                origin=record.origin,
                                status="uploaded",
                                detail=self._label(comparison),
                            )
                        except Exception as exc:  # noqa
                            import traceback

                            error(
                                "catalog write failed for "
                                f"{self._label(comparison)}: {exc}"
                            )
                            error(traceback.format_exc())
                            outcome = SyncOutcome(
                                key=comparison.key,
                                origin=record.origin,
                                status="failed",
                                detail=(
                                    f"{self._label(comparison)}: "
                                    f"catalog write: {exc}"
                                ),
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
                finally:
                    write_queue.task_done()

        processor_tasks = [
            asyncio.create_task(raw_processor()) for _ in range(workers)
        ]
        # Heavy sources (DadosGov multi-million-row archives) are
        # processed one at a time to bound peak memory; FTP files are
        # small and flow through the processor pool.
        gov_workers_tasks = [
            asyncio.create_task(gov_worker())
            for _ in range(2 if gov_items else 0)
        ]
        ftp_tasks = []
        if ftp_pool:
            step = max(1, len(ftp_pool))
            ftp_tasks = [
                asyncio.create_task(ftp_downloader(client, ftp_items[i::step]))
                for i, client in enumerate(ftp_pool)
            ]
        else:
            for _ in range(workers):
                await raw_queue.put(None)
        if not ftp_tasks and not gov_workers_tasks:
            for _ in range(workers):
                await write_queue.put(None)

        writers_total = len(gov_workers_tasks)
        writer_task = asyncio.create_task(catalog_writer())
        await asyncio.gather(*ftp_tasks, *gov_workers_tasks)
        for _ in processor_tasks:
            await raw_queue.put(None)
        await asyncio.gather(*processor_tasks)
        for _ in range(writers_total):
            await write_queue.put(None)
        await writer_task

        if self._changed_catalog and checkpoint_every is not None:
            await self._checkpoint()

        if save_snapshots:
            for origin, items in records.items():
                self.inventory.save_snapshot(origin, items)

        return report

    async def _convert_and_upload(
        self,
        file: BaseRemoteFile,
        raw_path: Path,
        callback: Callable[[int, int], None] | None = None,
    ) -> dict:
        """Convert a downloaded raw file, upload it, return the payload.

        The payload carries everything the catalog writer needs after the
        local files are removed.
        """
        from anyio import to_thread
        from pysus.api.extensions import ExtensionFactory

        s3_key = self.s3_key_for(file)
        raw_digest = await to_thread.run_sync(sha256_of, raw_path)

        local_file = await ExtensionFactory.instantiate(raw_path)
        if not hasattr(local_file, "to_parquet"):
            raise RuntimeError(f"{file.basename}: cannot convert to parquet")
        parquet_file = await local_file.to_parquet(callback=callback)
        try:
            parquet_digest = await to_thread.run_sync(
                sha256_of, parquet_file.path
            )
            payload = {
                "s3_key": s3_key,
                "size": parquet_file.path.stat().st_size,
                "rows": parquet_file.rows,
                "schema": parquet_file.schema,
                "raw_digest": raw_digest,
                "parquet_digest": parquet_digest,
            }
            await upload_s3(
                local_path=parquet_file.path,
                remote_path=s3_key,
                access_key=str(self.access_key),
                secret_key=str(self.secret_key),
                callback=callback,
            )
            return payload
        finally:
            self._cleanup_local(raw_path)
            self._cleanup_local(parquet_file.path)

    def _catalog_write_entry(
        self,
        adapter,
        central_adapter,
        columns_adapter,
        file: BaseRemoteFile,
        payload: dict,
    ) -> None:
        """Write one artifact's catalog rows in short transactions.

        All three catalogs are touched through short-lived direct
        DuckDB connections (``transaction()``), completely decoupled
        from engine lifecycles — DuckDB tears down the shared in-process
        database instance when its last connection closes, so no
        connection is ever held across operations here.
        """
        with central_adapter.transaction() as (
            central_conn,
            central_cursor,
        ):
            with columns_adapter.transaction() as (
                columns_conn,
                columns_cursor,
            ):
                with adapter.transaction() as (conn, dataset_cursor):
                    self.writer._ensure_management_columns(dataset_cursor)
                    self._catalog_rows(
                        central_cursor,
                        dataset_cursor,
                        columns_cursor,
                        file,
                        payload,
                    )
                    conn.commit()
                    dataset_cursor.execute("CHECKPOINT")
                columns_conn.commit()
            central_conn.commit()

    def _catalog_rows(
        self,
        central_cursor,
        dataset_cursor,
        columns_cursor,
        file: BaseRemoteFile,
        payload: dict,
    ) -> None:
        """Write dataset/group/file/column rows for an uploaded artifact."""
        writer = self.writer
        dataset_id = writer.ensure_dataset(
            central_cursor,
            file.dataset.name,
            file.dataset.long_name,
            getattr(file.dataset, "description", None),
        )

        group = getattr(file, "group", None)
        group_name = getattr(group, "name", None) if group is not None else None
        group_name = str(group_name) if group_name else None
        group_id = writer.ensure_group(
            dataset_cursor,
            dataset_id,
            group_name,
            getattr(group, "long_name", None) if group else None,
            getattr(group, "description", None) if group else None,
        )

        writer.upsert_file(
            dataset_cursor,
            dataset_id=dataset_id,
            group_id=group_id,
            path=payload["s3_key"],
            size=payload["size"],
            rows=payload["rows"],
            modified=datetime.now(),
            origin_modified=self._safe_modify(file),
            origin_size=self._safe_size(file),
            origin_path=str(file.path),
            year=file.year,
            month=file.month,
            state=file.state,
            origin=file.client.name.lower(),
            format="parquet",
            sha256=payload["parquet_digest"],
            source_sha256=payload["raw_digest"],
            file_type="PARQUET",
        )

        inserted = writer.get_file(dataset_cursor, payload["s3_key"])
        assert inserted is not None
        file_id, _ = inserted
        writer.link_columns(
            dataset_cursor,
            columns_cursor,
            file_id,
            payload["schema"],
            dataset_id,
        )

        if file.client.name == "saude":
            from pysus.api.saude.schemas import apply_column_descriptions

            apply_column_descriptions(
                columns_cursor,
                dataset_id,
                dataset=file.dataset.name.lower(),
                endpoint=file.basename.rsplit(".", 1)[0],
            )

    async def _preconnect_adapters(
        self, records: dict[str, list[FileRecord]]
    ) -> None:
        """Ensure all adapters are connected before parallel ingestion."""
        ducklake = self._require_ducklake()
        await ducklake.catalog_adapter.ensure_connected()
        await ducklake.columns_adapter.ensure_connected()
        datasets = {
            r.dataset.lower()
            for items in records.values()
            for r in items
            if r.origin in ("ftp", "dadosgov", "saude")
        }
        for name in datasets:
            await self._dataset_adapter_by_name(name).ensure_connected()

    async def _dedupe_s3_artifacts(
        self,
        ducklake_records: list[FileRecord],
    ) -> None:
        """Keep only the most updated S3 artifact per logical file.

        Legacy ETL runs stored the same logical file under multiple
        origin paths (e.g. ``public/data/ftp/...`` and
        ``public/data/dadosgov/...``). Grouping runs on the raw S3
        records — the comparator collapses same-origin entries, which
        would hide these duplicates. The newest artifact (by source
        modification date) survives; the others are deleted from the
        bucket and the catalog.
        """
        import boto3
        from botocore.config import Config

        groups: dict[tuple, list[FileRecord]] = {}
        for record in ducklake_records:
            key = (record.dataset.lower(), record.year, record.stem)
            groups.setdefault(key, []).append(record)

        s3 = boto3.client(
            "s3",
            endpoint_url="https://nbg1.your-objectstorage.com",
            region_name="nbg1",
            aws_access_key_id=str(self.access_key),
            aws_secret_access_key=str(self.secret_key),
            config=Config(signature_version="s3v4"),
        )

        ducklake = self._require_ducklake()
        for (dataset, _, _), artifacts in groups.items():
            if len(artifacts) < 2:
                continue

            newest = max(
                artifacts,
                key=lambda r: (
                    r.source_modified or r.modified or datetime.min,
                    r.size,
                ),
            )
            for old in artifacts:
                if old is newest:
                    continue
                adapter = ducklake.get_dataset_adapter(dataset)
                try:
                    with adapter.transaction() as (conn, cursor):
                        cursor.execute(
                            "DELETE FROM pysus.file_columns "
                            "WHERE file_id IN (SELECT id FROM pysus.files "
                            "WHERE path = ?)",
                            (old.path,),
                        )
                        cursor.execute(
                            "DELETE FROM pysus.files WHERE path = ?",
                            (old.path,),
                        )
                        conn.commit()
                        cursor.execute("CHECKPOINT")
                    adapter.mark_dirty()
                    s3.delete_object(Bucket="pysus", Key=str(old.path))
                    self._changed_catalog = True
                    print(
                        f"[deduped] removed {old.path} kept {newest.path}",
                        flush=True,
                    )
                except Exception as exc:  # noqa
                    error(f"dedup failed for {old.path}: {exc}")

    async def _fix_misparsed_metadata(
        self,
        ducklake_records: list[FileRecord],
    ) -> None:
        """Repair rows whose parsed month is invalid (part-file bug).

        The old SIA formatter glued part suffixes onto the month
        (``BIRJ2504_2`` → month ``42``). Such rows are reparsed from
        ``source_path`` with the fixed formatter; the object is moved to
        the corrected hierarchical key (alias kept at the old key) and
        the catalog row is updated.
        """
        import boto3
        from botocore.config import Config

        from .normalize import formatter_for
        from .records import compose_s3_key

        s3 = boto3.client(
            "s3",
            endpoint_url="https://nbg1.your-objectstorage.com",
            region_name="nbg1",
            aws_access_key_id=str(self.access_key),
            aws_secret_access_key=str(self.secret_key),
            config=Config(signature_version="s3v4"),
        )
        ducklake = self._require_ducklake()

        for record in ducklake_records:
            if record.month is None or record.month <= 12:
                continue
            formatter = formatter_for("ftp", record.dataset)
            if formatter is None:
                continue
            source_name = (
                Path(record.source_path).name
                if record.source_path
                else Path(record.path).name
            )
            try:
                parsed = formatter(source_name)
            except Exception:  # noqa
                continue
            group = parsed.get("group")
            group_long_name = None
            if isinstance(group, dict):
                group_long_name = group.get("long_name")
                group = group.get("name")
            month = parsed.get("month")
            if month is None or int(month) > 12:
                continue

            new_key = compose_s3_key(
                origin="ftp",
                dataset=record.dataset,
                name=source_name,
                group=group or record.group,
                year=parsed.get("year") or record.year,
                month=int(month),
                state=parsed.get("state") or record.state,
            )
            if new_key == record.path:
                continue

            adapter = ducklake.get_dataset_adapter(record.dataset)
            try:
                already_alias = None
                try:
                    head = s3.head_object(Bucket="pysus", Key=str(record.path))
                    already_alias = head.get("Metadata", {}).get("pysus-alias")
                except Exception:  # noqa
                    pass
                if already_alias != new_key:
                    s3.copy_object(
                        Bucket="pysus",
                        CopySource={
                            "Bucket": "pysus",
                            "Key": str(record.path),
                        },
                        Key=new_key,
                    )
                    from pysus.api.ducklake.functional import alias_marker

                    s3.put_object(
                        Bucket="pysus",
                        Key=str(record.path),
                        Body=alias_marker(new_key).encode(),
                        Metadata={"pysus-alias": new_key},
                    )
                with adapter.transaction() as (conn, cursor):
                    cursor.execute(
                        "SELECT dataset_id FROM pysus.files WHERE path = ?",
                        (record.path,),
                    )
                    row = cursor.fetchone()
                    dataset_id = row[0] if row else 0
                    group_id = self.writer.ensure_group(
                        cursor,
                        dataset_id,
                        str(group) if group else None,
                        group_long_name or (str(group) if group else None),
                    )
                    cursor.execute(
                        "UPDATE pysus.files SET path = ?, month = ?, "
                        "year = ?, state = ?, group_id = ? "
                        "WHERE path = ?",
                        (
                            new_key,
                            int(month),
                            parsed.get("year"),
                            (parsed.get("state") or record.state),
                            group_id,
                            record.path,
                        ),
                    )
                    conn.commit()
                    cursor.execute("CHECKPOINT")
                adapter.mark_dirty()
                self._changed_catalog = True
                print(
                    f"[repaired] {record.path} -> {new_key}",
                    flush=True,
                )
            except Exception as exc:  # noqa
                error(f"repair failed for {record.path}: {exc}")

    @staticmethod
    def _pick_source(comparison: FileComparison) -> FileRecord | None:
        """Return the artifact to ingest (ftp > dadosgov > saude)."""
        record = comparison._pick("ftp")
        if record is None or record.file is None:
            record = comparison._pick("dadosgov")
        if record is None or record.file is None:
            record = comparison._pick("saude")
        if record is None or record.file is None:
            return None
        return record

    @staticmethod
    def _label(comparison: FileComparison) -> str:
        key = comparison.key
        return (
            f"{key.dataset}/{key.group or '-'}/"
            f"{key.year or '-'}/{key.month or '-'}/{key.stem}"
        )

    async def _checkpoint(self) -> None:
        """Upload all dirty catalogs to S3 and reconnect the adapters."""
        ducklake = self._require_ducklake()
        await ducklake.flush_catalogs(update=True)
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
        """True when a non-S3 artifact certainly differs from the S3 copy.

        Trust-the-catalog policy: legacy ``origin_modified`` values are
        upload timestamps (unreliable for freshness), so modification
        dates are ignored on this pass. A file on S3 is only re-checked
        when the source *size* is known to differ from the recorded
        ``origin_size`` — size inequality proves the content changed,
        so no false skips are possible. Equal sizes (or unknown legacy
        sizes) trust the catalog; the content veto still guards any
        re-check that does happen.
        """
        s3_record = comparison._pick("ducklake")
        if s3_record is None:
            return False

        # origin_size is the raw source size recorded at upload time;
        # the parquet size can never equal the raw size.
        s3_size = s3_record.source_size
        if not s3_size:
            return False

        for record in comparison.records:
            if record.origin == "ducklake":
                continue
            if record.size and record.size != s3_size:
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
