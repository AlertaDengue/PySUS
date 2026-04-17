from collections.abc import Callable
from pathlib import Path
from typing import Any

import boto3
import httpx
from anyio import to_thread
from botocore.config import Config
from pydantic import BaseModel, PrivateAttr, SecretStr
from pysus import CACHEPATH
from pysus.api.models import BaseRemoteClient, BaseRemoteFile
from sqlalchemy import create_engine
from sqlalchemy.orm import joinedload, sessionmaker
from sqlalchemy.pool import StaticPool

from .catalog import CatalogDataset, CatalogFile, DatasetGroup
from .models import Dataset, File


class DuckLakeCredentials(BaseModel):
    access_key: SecretStr
    secret_key: SecretStr


class DuckLake(BaseRemoteClient):
    endpoint: str = "nbg1.your-objectstorage.com"
    region: str = "nbg1"
    bucket: str = "pysus"
    credentials: DuckLakeCredentials | None = None

    _cache_dir: Path = PrivateAttr()
    _catalog_local: Path = PrivateAttr()
    _catalog_remote: str = "public/catalog.db"
    _s3_client: Any = PrivateAttr(default=None)
    _engine: Any = PrivateAttr(default=None)
    _Session: Any = PrivateAttr(default=None)

    def __init__(self, engine=None, **data):
        super().__init__(**data)
        self._engine = engine
        self._cache_dir = Path(CACHEPATH) / "ducklake"
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._catalog_local = self._cache_dir / "catalog.db"

    @property
    def name(self) -> str:
        return "DuckLake"

    @property
    def long_name(self) -> str:
        return "PySUS s3 Client"

    @property
    def description(self) -> str:
        return ""  # TODO:

    @property
    def catalog_path(self) -> Path:
        return self._catalog_local

    @property
    def _catalog_url(self) -> str:
        return f"https://{self.endpoint}/{self.bucket}/{self._catalog_remote}"

    @property
    def _is_authenticated(self) -> bool:
        return self.credentials is not None

    async def datasets(self, **kwargs) -> list[Dataset]:
        if not self._Session:
            await self.connect()

        def _fetch():
            with self._Session() as session:
                results = (
                    session.query(CatalogDataset)
                    .options(
                        joinedload(CatalogDataset.groups).joinedload(
                            DatasetGroup.files
                        ),
                        joinedload(CatalogDataset.files),
                    )
                    .all()
                )
                session.expunge_all()
                return results

        records = await to_thread.run_sync(_fetch)
        return [Dataset(record=rec, client=self) for rec in records]

    async def login(
        self,
        access_key: str | None = None,
        secret_key: str | None = None,
        **kwargs,
    ) -> None:
        if access_key and secret_key:
            self.credentials = DuckLakeCredentials(
                access_key=SecretStr(access_key),
                secret_key=SecretStr(secret_key),
            )
        else:
            self.credentials = None

        await self.connect(force=True)

        if self._is_authenticated:
            self._s3_client = await to_thread.run_sync(
                self._get_s3_client,
            )

    def _setup_engine(self):
        engine = create_engine(
            f"duckdb:///{self._catalog_local}",
            poolclass=StaticPool,
        )

        with engine.connect() as conn:
            conn.exec_driver_sql("INSTALL ducklake; LOAD ducklake;")

            has_pysus = conn.exec_driver_sql(
                """
                SELECT 1 FROM information_schema.schemata WHERE
                schema_name = 'pysus'
            """
            ).fetchone()

            if has_pysus:
                conn.exec_driver_sql("SET search_path='pysus,main';")
            else:
                conn.exec_driver_sql("SET search_path='main';")

            s3_cfg = {
                "s3_endpoint": self.endpoint,
                "s3_region": self.region,
                "s3_url_style": "path",
                "s3_use_ssl": "true",
            }

            if self.credentials and self._is_authenticated:
                s3_cfg["s3_access_key_id"] = (
                    self.credentials.access_key.get_secret_value()
                )
                s3_cfg["s3_secret_access_key"] = (
                    self.credentials.secret_key.get_secret_value()
                )

            for key, value in s3_cfg.items():
                conn.exec_driver_sql(f"SET {key}='{value}';")

            conn.commit()

        return engine

    async def connect(self, force: bool = False):
        if self._engine and not force:
            if not self._Session:
                self._Session = sessionmaker(bind=self._engine)
            return

        await self._load_catalog()
        self._engine = await to_thread.run_sync(self._setup_engine)
        self._Session = sessionmaker(bind=self._engine)

    async def close(self):
        if self._engine:
            await to_thread.run_sync(self._engine.dispose)

            self._engine = None
            self._Session = None

            if self._is_authenticated:
                await self._upload_catalog()

            self._s3_client = None

    async def _download_file(
        self,
        file: BaseRemoteFile,
        output: Path,
        callback: Callable[[int], None] | None = None,
    ) -> Path:
        if not isinstance(file, File):
            raise ValueError("FTP File was not properly instantiated")

        url = f"https://{self.endpoint}/{self.bucket}/{file.record.path}"
        async with httpx.AsyncClient(follow_redirects=True) as client:
            async with client.stream("GET", url) as r:
                r.raise_for_status()
                with open(output, "wb") as f:
                    async for chunk in r.aiter_bytes(chunk_size=1024 * 1024):
                        await to_thread.run_sync(f.write, chunk)
                        if callback:
                            callback(len(chunk))
        return output

    async def _download_catalog(self, client: httpx.AsyncClient):
        async with client.stream("GET", self._catalog_url) as r:
            r.raise_for_status()
            with open(self._catalog_local, "wb") as f:
                async for chunk in r.aiter_bytes(chunk_size=1024 * 1024):
                    await to_thread.run_sync(f.write, chunk)

    def _get_s3_client(self):
        if not self.credentials:
            raise ConnectionError("S3 Credentials not found")
        return boto3.client(
            "s3",
            endpoint_url=f"https://{self.endpoint}",
            aws_access_key_id=self.credentials.access_key.get_secret_value(),
            aws_secret_access_key=(
                self.credentials.secret_key.get_secret_value()
            ),
            region_name=self.region,
            config=Config(signature_version="s3v4"),
        )

    async def _load_catalog(self):
        async with httpx.AsyncClient(follow_redirects=True) as client:
            local_size = -1
            if self._catalog_local.exists():
                try:
                    local_size = self._catalog_local.stat().st_size
                except OSError:
                    pass
            try:
                head = await client.head(self._catalog_url)
                head.raise_for_status()
                remote_size = int(head.headers.get("content-length", 0))
            except Exception:  # noqa: B902
                remote_size = 0
            if remote_size != local_size:
                await self._download_catalog(client)

    async def _upload_catalog(self):
        if not self._is_authenticated:
            raise PermissionError(
                "Admin credentials required to upload catalog.",
            )

        def _upload():
            self._s3_client.upload_file(
                str(self._catalog_local),
                self.bucket,
                self._catalog_remote,
            )

        await to_thread.run_sync(_upload)

    async def query(
        self,
        dataset: str | None = None,
        group: str | None = None,
        state: str | None = None,
        year: int | None = None,
        month: int | None = None,
    ) -> list[File]:
        if not self._Session:
            await self.connect()

        def _query():
            with self._Session() as session:
                q = session.query(CatalogFile).options(
                    joinedload(CatalogFile.dataset),
                    joinedload(CatalogFile.group),
                )

                if dataset:
                    q = q.join(CatalogDataset).filter(
                        CatalogDataset.name == dataset.lower()
                    )

                if group:
                    q = q.join(DatasetGroup).filter(DatasetGroup.name == group)

                if state:
                    q = q.filter(CatalogFile.state == state.upper())

                if year:
                    q = q.filter(CatalogFile.year == year)

                if month:
                    q = q.filter(CatalogFile.month == month)

                results = q.all()
                session.expunge_all()
                return results

        records = await to_thread.run_sync(_query)
        return [
            File(path=r.path, record=r, dataset=r.dataset, group=r.group)
            for r in records
        ]
