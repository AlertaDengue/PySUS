from abc import ABC
from pathlib import Path
import asyncio

import httpx
from anyio import to_thread
from pydantic import BaseModel, SecretStr
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import sessionmaker, Session

from pysus import CACHEPATH
from pysus.api import types
from pysus.api.ducklake.functional import download_s3, upload_s3
from pysus.api.ducklake.catalog.orm.dataset import DatasetBase


class DuckLakeCredentials(BaseModel):
    access_key: SecretStr
    secret_key: SecretStr


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
        self._session_factory = None
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.credentials = credentials
        self.update_on_close = update_on_close

    @property
    def remote_url(self) -> str:
        return f"https://{types.S3_ENDPOINT}/{types.S3_BUCKET}/{self.db_remote}"

    def get_session(self) -> Session:
        if not self._session_factory:
            raise RuntimeError("Database engine not initialized. Call connect() first.")
        return self._session_factory()

    async def connect(self, force: bool = False) -> None:
        if self._engine and not force:
            if not self._session_factory:
                self._session_factory = sessionmaker(bind=self._engine)
            return

        await self._download_catalog(
            self.db_local,
            str(self.db_remote),
        )
        self._engine = await to_thread.run_sync(self.setup_engine)
        self._session_factory = sessionmaker(bind=self._engine)

    def setup_engine(
        self, access_key: str | None = None, secret_key: str | None = None
    ) -> Engine:
        engine: Engine = create_engine(
            f"duckdb:///{self.db_local}",
            poolclass=StaticPool,
        )

        with engine.connect() as conn:
            conn.exec_driver_sql("INSTALL ducklake; LOAD ducklake;")
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS pysus;")

            has_pysus = conn.exec_driver_sql(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pysus'"
            ).fetchone()

            if has_pysus:
                conn.exec_driver_sql("SET search_path='pysus,main';")
            else:
                conn.exec_driver_sql("SET search_path='main';")

            s3_cfg = {
                "s3_endpoint": types.S3_ENDPOINT,
                "s3_region": types.S3_REGION,
                "s3_url_style": "path",
                "s3_use_ssl": "true",
            }

            if access_key and secret_key:
                s3_cfg["s3_access_key_id"] = access_key
                s3_cfg["s3_secret_access_key"] = secret_key

            for key, value in s3_cfg.items():
                conn.exec_driver_sql(f"SET {key}='{value}'")

            conn.commit()

        DatasetBase.metadata.create_all(bind=engine)
        return engine

    async def _download_catalog(self, local_path: Path, remote_path: str) -> None:
        url = f"https://{types.S3_ENDPOINT}/{types.S3_BUCKET}/{remote_path}"

        if local_path.exists():
            try:
                local_size = local_path.stat().st_size
            except OSError:
                local_size = -1
        else:
            local_size = -1

        async with httpx.AsyncClient(follow_redirects=True) as client:
            try:
                head = await client.head(url)

                if head.status_code == 404:
                    return

                head.raise_for_status()
                remote_size = int(head.headers.get("content-length", 0))
            except httpx.HTTPStatusError:
                return
            except Exception:
                remote_size = 0

        if remote_size == local_size:
            return

        access_key = (
            self.credentials.access_key.get_secret_value() if self.credentials else None
        )
        secret_key = (
            self.credentials.secret_key.get_secret_value() if self.credentials else None
        )

        await download_s3(
            remote_path=remote_path,
            local_path=local_path,
            access_key=access_key,
            secret_key=secret_key,
        )

    async def _upload_catalog(self) -> None:
        if not self.credentials:
            raise PermissionError(
                "Admin credentials required to upload catalog.",
            )

        if not self.db_local.exists():
            raise FileNotFoundError("catalog file not found")

        await upload_s3(
            local_path=self.db_local,
            remote_path=str(self.db_remote),
            access_key=self.credentials.access_key.get_secret_value(),
            secret_key=self.credentials.secret_key.get_secret_value(),
        )

    async def close(self, update: bool = False) -> None:
        if update:
            await self._upload_catalog()

        if self._engine:
            await to_thread.run_sync(self._engine.dispose)
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
        self.db_remote: str = "public/catalog.duckdb"


class DatasetAdapter(BaseAdapter):
    def __init__(self, name: str, engine=None, **data) -> None:
        super().__init__(engine=engine, **data)
        self.dataset_name: str = name
        self.db_local: Path = self.cache_dir / f"catalog_{name}.duckdb"
        self.db_remote: str = f"datasets/catalog_{name}.duckdb"


class ColumnsAdapter(BaseAdapter):
    def __init__(self, engine=None, **data) -> None:
        super().__init__(engine=engine, **data)
        self.db_local: Path = self.cache_dir / "columns.duckdb"
        self.db_remote: str = "public/columns.duckdb"

