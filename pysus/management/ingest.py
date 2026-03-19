import requests
from typing import Literal, List
from pathlib import Path

import boto3
import duckdb
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from botocore.config import Config

from pysus import CACHEPATH
from pysus.api.ducklake.models import Dataset, DatasetGroup, File, DatasetMetadata
from pysus.api.ftp import File as FTPFile
from pysus.api.dadosgov.models import Resource


class S3Client:
    def __init__(self, access_key: str, secret_key: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = "pysus"
        self.endpoint = "nbg1.your-objectstorage.com"
        self.catalog_local = CACHEPATH / "catalog.db"
        self.catalog_remote = "public/catalog.db"

        self.s3 = boto3.client(
            "s3",
            endpoint_url=f"https://{self.endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="nbg1",
            config=Config(signature_version="s3v4"),
        )
        self.db = None

    def __enter__(self):
        self.download_catalog()
        self.db = duckdb.connect()
        self._configure_duckdb()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db:
            self.db.close()
        if exc_type is None:
            self.upload_catalog()

    @property
    def catalog_url(self) -> str:
        return f"https://{self.endpoint}/{self.bucket}/{self.catalog_remote}"

    def _configure_duckdb(self):
        self.db.execute("INSTALL ducklake; LOAD ducklake;")
        self.db.execute(f"""
            SET s3_endpoint='{self.endpoint}';
            SET s3_region='nbg1';
            SET s3_url_style='path';
            SET s3_use_ssl=true;
            SET s3_access_key_id='{self.access_key}';
            SET s3_secret_access_key='{self.secret_key}';
        """)
        self.db.execute(f"ATTACH 'ducklake:{self.catalog_local}' AS pysus;")
        self.db.execute("USE pysus;")

    def download_catalog(self):
        self.catalog_local.parent.mkdir(parents=True, exist_ok=True)
        try:
            r = requests.get(self.catalog_url)
            r.raise_for_status()
            with self.catalog_local.open("wb") as f:
                f.write(r.content)
        except requests.exceptions.RequestException:
            pass

    def upload_catalog(self):
        self.s3.upload_file(
            str(self.catalog_local),
            self.bucket,
            self.catalog_remote,
        )


class Ingestor:
    def __init__(
        self,
        client: S3Client,
    ):
        self.client = client
        self.session = sessionmaker(
            bind=create_engine(f"duckdb:///{client.catalog_local}")
        )

    def ingest(
        self,
        origin: Literal["ftp", "dadosgov"],
        file: FTPFile | Resource,
        force: bool = False,
    ) -> None: ...

    def bulk_ingest(
        self,
        origin: Literal["ftp", "dadosgov"],
        files: List[FTPFile | Resource],
    ) -> None: ...

    def _ftp_ingest(self, file: FTPFile) -> None: ...

    def _dadosgov_ingest(self, file: Resource) -> None: ...

    def _should_insert(self, file: FTPFile | Resource) -> bool: ...

    def _download_file(self, file: FTPFile | Resource) -> Path: ...

    def _extract_metadata(self, file: FTPFile | Resource) -> File: ...

    def _upload_parquet(self, parquet: Path, metadata: File) -> None: ...
