import asyncio
import os
from abc import ABC
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path

import httpx
from anyio import to_thread
from pydantic import BaseModel, SecretStr
from pysus import CACHEPATH
from pysus.api import types
from pysus.api.ducklake.catalog.orm.dataset import DatasetBase
from pysus.api.ducklake.functional import download_http, upload_s3
from pysus.api.errors import CatalogError
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Result
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool


class DuckLakeCredentials(BaseModel):
    access_key: SecretStr
    secret_key: SecretStr


_SHARED_ENGINES: dict[str, Engine] = {}


def _dispose_shared(db_local: Path) -> None:
    """Dispose and forget the shared engine for *db_local*, if any.

    The shared engine is also the process-lifetime anchor: its pooled
    connection keeps the DuckDB instance alive, so every other adapter
    attached to the same file stays valid. Dispose only happens right
    before the file is replaced by a re-download.
    """
    key = str(db_local.resolve())
    engine = _SHARED_ENGINES.pop(key, None)
    if engine is not None:
        try:
            engine.dispose()
        except Exception:  # noqa
            pass


class BaseAdapter(ABC):
    cache_dir: Path = Path(CACHEPATH) / "ducklake"
    db_local: Path
    db_remote: Path

    def __init__(
        self,
        engine=None,
        credentials: DuckLakeCredentials | None = None,
        update_on_close: bool = False,
        **data,
    ) -> None:
        self._engine = engine
        self._session_factory: sessionmaker[Session] | None = None
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.credentials = credentials
        self.update_on_close = update_on_close
        self._local_dirty = False

    @property
    def remote_url(self) -> str:
        return f"https://{types.S3_ENDPOINT}/{types.S3_BUCKET}/{self.db_remote}"

    @property
    def connected(self) -> bool:
        """True when the database engine is initialized."""
        return self._engine is not None

    @property
    def local_dirty(self) -> bool:
        """True when the local database has unsaved changes."""
        return self._local_dirty

    def mark_dirty(self) -> None:
        """Flag the local database as modified (upload on close)."""
        self._local_dirty = True

    async def ensure_connected(
        self,
        callback: Callable[[int, int], None] | None = None,
    ) -> None:
        """Connect the engine if it is not already connected.

        The shared engine doubles as the process-lifetime anchor, so the
        DuckDB instance can never be torn down by other lifecycle paths.
        """
        if self._engine is not None:
            return
        await self.connect(callback=callback)

    def checkpoint(self) -> None:
        """Force a WAL checkpoint, persisting writes to ``db_local``."""
        self.setup_engine().raw_connection().execute("CHECKPOINT")

    async def reconnect(self) -> None:
        """Dispose the shared engine and reinitialize from scratch.

        Used after the database file is found broken or right before it
        is replaced by a fresh download.
        """
        _dispose_shared(self.db_local)
        self._engine = None
        self._session_factory = None
        await self.connect(force=True)

    def raw_connection(self):
        """Return a raw DuckDB connection to the catalog database.

        Raises
        ------
        CatalogError
            If the engine is not initialized (call
            :meth:`ensure_connected` first).
        """
        if self._engine is None:
            raise CatalogError(
                "Database engine not initialized. "
                "Call ensure_connected() first."
            )
        return self._engine.raw_connection()

    @contextmanager
    def transaction(self):
        """Yield ``(connection, cursor)`` for a committed transaction.

        Uses the shared engine's pooled connection — the only way to
        guarantee every connection to a DuckDB file carries identical
        configuration (DuckDB rejects mixed-configuration opens) and to
        keep the process-lifetime instance anchored. The connection is
        *not* closed (it is the shared anchor); the transaction is
        committed on success and rolled back on error.
        """
        engine = self.setup_engine()
        conn = engine.raw_connection()
        try:
            probe = conn.cursor()
            try:
                probe.execute("SELECT 1")
                probe.fetchone()
            except Exception as exc:  # noqa
                raise CatalogError(
                    f"Catalog connection is broken: {exc}"
                ) from exc
            with conn:
                cursor = conn.cursor()
                yield conn, cursor
        finally:
            pass

    def get_session(self) -> Session:
        if not self._session_factory:
            raise CatalogError(
                "Database engine not initialized. Call connect() first."
            )
        return self._session_factory()

    def sql(self, query: str, params: dict | None = None) -> Result:
        if not self._engine:
            raise CatalogError(
                "Database engine not initialized. Call connect() first."
            )
        with self._engine.connect() as conn:
            if params:
                return conn.execute(text(query), params)
            return conn.exec_driver_sql(query)

    async def connect(
        self,
        force: bool = False,
        callback: Callable[[int, int], None] | None = None,
    ) -> None:
        if self._engine and not force:
            if not self._session_factory:
                self._session_factory = sessionmaker(bind=self._engine)
            return

        if force:
            _dispose_shared(self.db_local)
            await self._download_catalog(
                self.db_local,
                str(self.db_remote),
                force=True,
                callback=callback,
            )
            self._local_dirty = False
            self._engine = await to_thread.run_sync(self.setup_engine)
            self._session_factory = sessionmaker(bind=self._engine)
            return

        if self._local_dirty:
            self._engine = await to_thread.run_sync(self.setup_engine)
            self._session_factory = sessionmaker(bind=self._engine)
            return

        await self._download_catalog(
            self.db_local,
            str(self.db_remote),
            force=False,
            callback=callback,
        )
        try:
            self._engine = await to_thread.run_sync(self.setup_engine)
            self._session_factory = sessionmaker(bind=self._engine)
        except Exception:  # noqa
            _dispose_shared(self.db_local)
            if self.db_local.exists():
                try:
                    os.remove(self.db_local)
                except OSError:
                    pass

            await self._download_catalog(
                self.db_local,
                str(self.db_remote),
                force=True,
                callback=callback,
            )
            self._engine = await to_thread.run_sync(self.setup_engine)
            self._session_factory = sessionmaker(bind=self._engine)

    def setup_engine(
        self, access_key: str | None = None, secret_key: str | None = None
    ) -> Engine:
        """Return the shared engine for this adapter's database file.

        DuckDB keeps one database instance per file per process; opening
        the file again merely attaches to it, and disposing one engine's
        connection tears the shared instance down for everyone else.
        Adapters therefore share one engine per file (process-wide) that
        is only disposed via :func:`_dispose_shared` — right before the
        file itself is replaced by a re-download.
        """
        key = str(self.db_local.resolve())
        engine = _SHARED_ENGINES.get(key)
        if engine is not None:
            return engine

        engine = create_engine(
            f"duckdb:///{self.db_local}",
            poolclass=StaticPool,
        )

        with engine.connect() as conn:
            conn.exec_driver_sql("INSTALL ducklake; LOAD ducklake;")
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS pysus;")
            conn.commit()

        DatasetBase.metadata.create_all(bind=engine)
        _SHARED_ENGINES[key] = engine
        return engine

    async def _download_catalog(
        self,
        local_path: Path,
        remote_path: str,
        force: bool = False,
        callback: Callable[[int, int], None] | None = None,
    ) -> None:
        remote = str(remote_path).replace("\\", "/")
        url = f"https://{types.S3_ENDPOINT}/{types.S3_BUCKET}/{remote}"

        if local_path.exists() and not force:
            try:
                local_size = local_path.stat().st_size
            except OSError:
                local_size = -1
        else:
            local_size = -1

        remote_size = 0
        if local_size != -1:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Referer": "https://github.com/AlertaDengue/PySUS",
            }
            async with httpx.AsyncClient(
                headers=headers, follow_redirects=True
            ) as client:
                try:
                    head = await client.head(url)

                    if head.status_code == 404:
                        return

                    head.raise_for_status()
                    remote_size = int(head.headers.get("content-length", 0))
                except httpx.HTTPStatusError:
                    return
                except Exception:  # noqa
                    remote_size = 0

        if not force and remote_size == local_size and local_size != -1:
            return

        try:
            await download_http(
                remote_path=remote,
                local_path=local_path,
                callback=callback,
            )
        except Exception:  # noqa: B902
            if local_path.exists():
                try:
                    local_path.unlink()
                except OSError:
                    pass
            raise

    async def _upload_catalog(self) -> None:
        if not self.credentials:
            raise PermissionError(
                "Admin credentials required to upload catalog.",
            )

        if not self.db_local.exists():
            raise FileNotFoundError("catalog file not found")

        # persist pending writes before uploading the file
        self.checkpoint()

        await upload_s3(
            local_path=self.db_local,
            remote_path=str(self.db_remote),
            access_key=self.credentials.access_key.get_secret_value(),
            secret_key=self.credentials.secret_key.get_secret_value(),
        )

    async def close(self, update: bool = False) -> None:
        if update and self._local_dirty:
            await self._upload_catalog()
            self._local_dirty = False

        # The engine is shared process-wide per database file and is
        # never disposed here: DuckDB tears down the in-process database
        # instance when its last connection closes, which would break
        # every other adapter attached to the same file. The shared
        # registry releases engines only via _dispose_shared() right
        # before a file is replaced.
        self._engine = None
        self._session_factory = None

    def __del__(self) -> None:
        if not hasattr(self, "_engine") or not self._engine:
            return
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                loop.create_task(self.close(update=False))
        except RuntimeError:
            try:
                asyncio.run(self.close(update=False))
            except Exception:  # noqa
                pass
        except Exception:  # noqa
            pass

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close(update=self.update_on_close)


class CatalogAdapter(BaseAdapter):
    def __init__(self, engine=None, **data) -> None:
        super().__init__(engine=engine, **data)
        self.db_local: Path = self.cache_dir / "catalog.duckdb"
        self.db_remote: Path = Path("public/catalog.duckdb")


class DatasetAdapter(BaseAdapter):
    def __init__(self, name: str, dataset_id: int, engine=None, **data) -> None:
        super().__init__(engine=engine, **data)
        self.dataset_name: str = name
        self.db_local: Path = self.cache_dir / f"catalog_{name}.duckdb"
        self.db_remote: Path = Path(f"public/catalog_{name}.duckdb")
        self.dataset_id = dataset_id


class ColumnsAdapter(BaseAdapter):
    def __init__(self, engine=None, **data) -> None:
        super().__init__(engine=engine, **data)
        self.db_local: Path = self.cache_dir / "catalog_columns.duckdb"
        self.db_remote: Path = Path("public/catalog_columns.duckdb")
