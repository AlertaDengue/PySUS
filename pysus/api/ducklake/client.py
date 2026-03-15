import requests
from pathlib import Path

import duckdb

from pysus import CACHEPATH


class DuckLake:
    def __init__(self):
        self.endpoint = "nbg1.your-objectstorage.com"
        self.remote_url = f"https://{self.endpoint}/pysus/public/catalog.db"
        self.cache_dir = Path(CACHEPATH) / "ducklake"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.catalog_local = self.cache_dir / "catalog.db"
        self._ensure_catalog()
        self.con = self._connect()

    def _remote_size(self):
        r = requests.head(self.remote_url)
        r.raise_for_status()
        return int(r.headers.get("content-length", 0))

    def _local_size(self):
        if not self.catalog_local.exists():
            return None
        return self.catalog_local.stat().st_size

    def _download_catalog(self):
        r = requests.get(self.remote_url, stream=True)
        r.raise_for_status()
        with open(self.catalog_local, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

    def _ensure_catalog(self):
        if self._remote_size() != self._local_size():
            self._download_catalog()

    def _connect(self):
        con = duckdb.connect()
        con.execute(
            f"""
            SET s3_endpoint='{self.endpoint}';
            SET s3_region='nbg1';
            SET s3_url_style='path';
            SET s3_use_ssl=true;
            """
        )
        con.execute(
            f"""
            ATTACH 'ducklake:{self.catalog_local}' AS pysus;
            USE pysus;
            """
        )
        return con
