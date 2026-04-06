from typing import Optional, Any, List
from pathlib import Path
import httpx
import duckdb

import anyio
import boto3
from botocore.config import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, joinedload
from pydantic import PrivateAttr, SecretStr, BaseModel

from pysus import CACHEPATH
from pysus.api.models import (
    BaseLocalFile,
    BaseRemoteClient,
)
from .models import CatalogDataset, CatalogFile
from .catalog import Dataset, DatasetGroup


class DuckLakeCredentials(BaseModel):
    access_key: SecretStr
    secret_key: SecretStr


class DuckLake(BaseRemoteClient):
    endpoint: str = "nbg1.your-objectstorage.com"
    region: str = "nbg1"
    bucket: str = "pysus"
    credentials: Optional[DuckLakeCredentials] = None

    _cache_dir: Path = PrivateAttr()
    _catalog_local: Path = PrivateAttr()
    _catalog_remote: str = "public/catalog.db"
    _con: Optional[duckdb.DuckDBPyConnection] = PrivateAttr(default=None)
    _s3_client: Any = PrivateAttr(default=None)
    _engine: Any = PrivateAttr(default=None)
    _Session: Any = PrivateAttr(default=None)

    def __init__(self, **data):
        super().__init__(**data)
        self._cache_dir = Path(CACHEPATH) / "ducklake"
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._catalog_local = self._cache_dir / "catalog.db"

    @property
    def _catalog_url(self) -> str:
        return f"https://{self.endpoint}/{self.bucket}/{self._catalog_remote}"

    @property
    def _is_authenticated(self) -> bool:
        return self.credentials is not None

    async def datasets(self, **kwargs) -> List[CatalogDataset]:
        if not self._Session:
            await self.connect()

        def _fetch():
            with self._Session() as session:
                return (
                    session.query(Dataset)
                    .options(
                        joinedload(Dataset.dataset_metadata),
                        joinedload(Dataset.groups).joinedload(
                            DatasetGroup.files),
                    )
                    .all()
                )

        records = await anyio.to_thread.run_sync(_fetch)
        return [CatalogDataset(record=rec, client=self) for rec in records]

    async def login(
        self,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
    ) -> None:
        if access_key and secret_key:
            self.credentials = DuckLakeCredentials(
                access_key=SecretStr(access_key),
                secret_key=SecretStr(secret_key),
            )
        else:
            self.credentials = None

        if self._con:
            await self.close()

        self._con = await anyio.to_thread.run_sync(self._create_connection)

        if self._is_authenticated:
            self._s3_client = await anyio.to_thread.run_sync(self._get_s3_client)

    async def connect(self, force: bool = False):
        if self._con and not force:
            return

        await self._load_catalog()

        self._con = await anyio.to_thread.run_sync(self._create_connection)

        self._engine = create_engine(f"duckdb:///{self._catalog_local}")
        self._Session = sessionmaker(bind=self._engine)

    async def close(self):
        if self._con:
            if self._is_authenticated:
                await self._upload_catalog()

            await anyio.to_thread.run_sync(self._con.close)
            self._con = None
            self._s3_client = None

    async def upload(self, file: BaseLocalFile, remote_path: str, **kwargs):
        if not self._is_authenticated:
            raise PermissionError("Authentication required")

        def _upload():
            self._s3_client.upload_file(
                str(file.path),
                self.bucket,
                remote_path,
            )

        await anyio.to_thread.run_sync(_upload)

    async def _download_file(self, file: "CatalogFile", output: Path) -> Path:
        url = f"https://{self.endpoint}/{self.bucket}/{file.record.path}"
        async with httpx.AsyncClient(follow_redirects=True) as client:
            async with client.stream("GET", url) as r:
                r.raise_for_status()

                def _write():
                    with open(output, "wb") as f:
                        for chunk in r.iter_bytes(chunk_size=1024 * 1024):
                            f.write(chunk)

                await anyio.to_thread.run_sync(_write)
        return output

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
            except Exception:
                remote_size = 0

            if remote_size != local_size:
                await self._download_catalog(client)

    async def _download_catalog(self, client: httpx.AsyncClient):
        async with client.stream("GET", self._catalog_url) as r:
            r.raise_for_status()

            def _write():
                with open(self._catalog_local, "wb") as f:
                    for chunk in r.iter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)

            await anyio.to_thread.run_sync(_write)

    def _get_s3_client(self):
        return boto3.client(
            "s3",
            endpoint_url=f"https://{self.endpoint}",
            aws_access_key_id=self.credentials.access_key.get_secret_value(),
            aws_secret_access_key=self.credentials.secret_key.get_secret_value(),
            region_name=self.region,
            config=Config(signature_version="s3v4"),
        )

    def _create_connection(self):
        con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        con.execute("INSTALL ducklake; LOAD ducklake;")

        s3_cfg = {
            "s3_endpoint": self.endpoint,
            "s3_region": self.region,
            "s3_url_style": "path",
            "s3_use_ssl": "true",
        }

        if self._is_authenticated:
            s3_cfg["s3_access_key_id"] = self.credentials.access_key.get_secret_value()
            s3_cfg["s3_secret_access_key"] = (
                self.credentials.secret_key.get_secret_value()
            )

        for key, value in s3_cfg.items():
            con.execute(f"SET {key}='{value}';")

        mode = "" if self._is_authenticated else "(READ_ONLY)"
        con.execute(f"ATTACH 'ducklake:{self._catalog_local}' AS pysus {mode};")
        con.execute("USE pysus;")
        return con

    async def _upload_catalog(self):
        if not self._is_authenticated:
            raise PermissionError(
                "Admin credentials required to upload catalog.")

        def _upload():
            self._s3_client.upload_file(
                str(self._catalog_local),
                self.bucket,
                self._catalog_remote,
            )

        await anyio.to_thread.run_sync(_upload)
