"""High-level client for DuckLake S3-based public health dataset catalog.

Provides authentication, dataset discovery, and file download
capabilities backed by per-dataset DuckDB engines.
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import boto3
import httpx
from anyio import sleep, to_thread
from botocore.config import Config
from pydantic import BaseModel, PrivateAttr, SecretStr
from pysus import CACHEPATH
from pysus.api.models import BaseRemoteClient, BaseRemoteFile
from pysus.api.types import DUCKLAKE
from sqlalchemy import create_engine
from sqlalchemy.orm import joinedload, sessionmaker
from sqlalchemy.pool import StaticPool

from .catalog import CatalogDataset, DatasetGroup
from .models import DuckDataset, File


class DuckLakeCredentials(BaseModel):
    """Credentials for authenticating with the S3-compatible object storage.

    Parameters
    ----------
    access_key : SecretStr
        The S3 access key ID.
    secret_key : SecretStr
        The S3 secret access key.
    """

    access_key: SecretStr
    secret_key: SecretStr


class DuckLake(BaseRemoteClient):
    """Client for the DuckLake S3-based public health dataset catalog.

    Parameters
    ----------
    endpoint : str, optional
        S3-compatible object storage endpoint.
    region : str, optional
        Storage region name.
    bucket : str, optional
        Bucket name containing the catalog.
    credentials : DuckLakeCredentials, optional
        Credentials for authenticated S3 operations.
    """

    endpoint: str = "nbg1.your-objectstorage.com"
    region: str = "nbg1"
    bucket: str = "pysus"
    credentials: DuckLakeCredentials | None = None

    _s3_client: Any = PrivateAttr(default=None)
    _Session: Any = PrivateAttr(default=None)

    def __init__(self, engine=None, **data) -> None:
        """Initialize the DuckLake client.

        Parameters
        ----------
        engine : object, optional
            Pre-configured SQLAlchemy engine for the discovery catalog.
        ``**data``
            Fields passed to the Pydantic base model.
        """
        super().__init__(**data)
        self._engine = engine
        self._cache_dir: Path = Path(CACHEPATH) / "ducklake"
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._catalog_local: Path = self._cache_dir / "catalog.db"
        self._catalog_remote: str = "public/catalog.db"

    @property
    def name(self) -> str:
        """Return the short name of this client.

        Returns
        -------
        str
            The client short name.
        """
        return DUCKLAKE

    @property
    def long_name(self) -> str:
        """Return the human-readable name of this client.

        Returns
        -------
        str
            The client display name.
        """
        return "PySUS s3 Client"

    @property
    def description(self) -> str:
        """Return a description of this client.

        Returns
        -------
        str
            A description string (currently empty).
        """
        return ""  # TODO:

    @property
    def catalog_path(self) -> Path:
        """Return the local path to the discovery catalog database.

        Returns
        -------
        Path
            Filesystem path to the local discovery catalog file.
        """
        return self._catalog_local

    @property
    def _catalog_url(self) -> str:
        """Return the remote URL of the discovery catalog."""
        return f"https://{self.endpoint}/{self.bucket}/{self._catalog_remote}"

    @property
    def _is_authenticated(self) -> bool:
        """Return whether the client has credentials configured."""
        return self.credentials is not None

    async def datasets(self, **kwargs) -> list[DuckDataset]:
        """Return all datasets from the catalog as DuckDataset instances.

        Parameters
        ----------
        ``**kwargs``
            Additional filter arguments (currently unused).

        Returns
        -------
        list[DuckDataset]
            List of all datasets in the catalog.
        """
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
        return [DuckDataset(record=rec, client=self) for rec in records]

    async def login(
        self,
        access_key: str | None = None,
        secret_key: str | None = None,
        **kwargs,
    ) -> None:
        """Authenticate with S3 credentials and reconnect to the catalog.

        Parameters
        ----------
        access_key : str, optional
            S3 access key ID. If omitted, credentials are cleared.
        secret_key : str, optional
            S3 secret access key. If omitted, credentials are cleared.
        ``**kwargs``
            Additional arguments (currently unused).
        """
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

    def _setup_engine(self, local_path: Path | None = None):
        """Create and configure a DuckDB engine with S3 settings.

        Parameters
        ----------
        local_path : Path, optional
            Path to the catalog database file. Defaults to the discovery catalog.
        """
        if local_path is None:
            local_path = self._catalog_local
        engine = create_engine(
            f"duckdb:///{local_path}",
            poolclass=StaticPool,
        )

        with engine.connect() as conn:
            conn.exec_driver_sql("INSTALL ducklake; LOAD ducklake;")

            has_pysus = conn.exec_driver_sql(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pysus'"
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
                conn.exec_driver_sql(f"SET {key}='{value}'")

            conn.commit()

        return engine

    async def connect(self, force: bool = False) -> None:
        """Connect to the discovery catalog, downloading first if needed.

        Parameters
        ----------
        force : bool, optional
            Whether to re-download and re-connect even if already connected.
        """
        if self._engine and not force:
            if not self._Session:
                self._Session = sessionmaker(bind=self._engine)
            return

        await self._download_catalog(
            self._catalog_local,
            self._catalog_remote,
        )
        self._engine = await to_thread.run_sync(self._setup_engine)
        self._Session = sessionmaker(bind=self._engine)

    async def close(self) -> None:
        """Dispose the discovery engine."""
        if self._engine:
            await to_thread.run_sync(self._engine.dispose)
            self._engine = None
            self._Session = None
            self._s3_client = None

    async def _download(
        self,
        remote_path: str,
        local_path: Path,
        *,
        callback: Callable[[int, int], None] | None = None,
    ) -> None:
        """Download *remote_path* to *local_path* with streaming and retries.

        Parameters
        ----------
        remote_path : str
            Object key within the bucket.
        local_path : Path
            Local destination path.
        callback : Callable[[int, int], None], optional
            Progress callback receiving ``(downloaded, total)`` bytes.
        """
        url = f"https://{self.endpoint}/{self.bucket}/{remote_path}"
        max_retries = 5

        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(follow_redirects=True) as client:
                    async with client.stream("GET", url) as r:
                        r.raise_for_status()
                        total = int(r.headers.get("Content-Length", 0))
                        downloaded = 0
                        with open(local_path, "wb") as f:
                            async for chunk in r.aiter_bytes(
                                chunk_size=1024 * 1024,
                            ):
                                await to_thread.run_sync(f.write, chunk)
                                downloaded += len(chunk)
                                if callback:
                                    callback(downloaded, total)
                return
            except OSError as e:
                if attempt < max_retries - 1:
                    await sleep(1)
                else:
                    raise e

    async def _download_catalog(self, local_path: Path, remote_path: str) -> None:
        """Download a catalog database from remote storage with retries.

        Parameters
        ----------
        local_path : Path
            Local destination path for the catalog file.
        remote_path : str
            Remote object key within the bucket.
        """
        url = f"https://{self.endpoint}/{self.bucket}/{remote_path}"

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
                head.raise_for_status()
                remote_size = int(head.headers.get("content-length", 0))
            except Exception:  # noqa: B902
                remote_size = 0

        if remote_size == local_size:
            return

        await self._download(remote_path, local_path)

    async def _download_file(
        self,
        file: BaseRemoteFile,
        output: Path,
        callback: Callable[[int, int], None] | None = None,
    ) -> Path:
        """Download a single file from object storage to the local path."""
        if not isinstance(file, File):
            raise ValueError("FTP File was not properly instantiated")

        await self._download(file.record.path, output, callback=callback)
        return output

    def _get_s3_client(self):
        """Create and return a boto3 S3 client for the configured endpoint."""
        if not self.credentials:
            raise ConnectionError("S3 Credentials not found")
        return boto3.client(
            "s3",
            endpoint_url=f"https://{self.endpoint}",
            aws_access_key_id=self.credentials.access_key.get_secret_value(),
            aws_secret_access_key=(self.credentials.secret_key.get_secret_value()),
            region_name=self.region,
            config=Config(signature_version="s3v4"),
        )


DuckDataset.model_rebuild(_types_namespace={"DuckLake": DuckLake})
