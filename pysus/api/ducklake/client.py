import asyncio
from pathlib import Path
from typing import Optional

import duckdb
import httpx
from pydantic import PrivateAttr

from pysus import CACHEPATH
from pysus.api.models import BaseRemoteClient


class DuckLake(BaseRemoteClient):
    endpoint: str = "nbg1.your-objectstorage.com"
    region: str = "nbg1"

    _cache_dir: Path = PrivateAttr()
    _catalog_local: Path = PrivateAttr()
    _con: Optional[duckdb.DuckDBPyConnection] = PrivateAttr(default=None)

    def __init__(self, **data):
        super().__init__(**data)
        self._cache_dir = Path(CACHEPATH) / "ducklake"
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._catalog_local = self._cache_dir / "catalog.db"

    @property
    def catalog_url(self) -> str:
        return f"https://{self.endpoint}/pysus/public/catalog.db"

    async def _download_catalog(self, client: httpx.AsyncClient):
        async with client.stream("GET", self.catalog_url) as r:
            r.raise_for_status()
            with open(self._catalog_local, "wb") as f:
                async for chunk in r.aiter_bytes(chunk_size=1024 * 1024):
                    f.write(chunk)

    def _connect(self):
        self._con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        self._con.execute(f"""
            SET s3_endpoint='{self.endpoint}';
            SET s3_region='{self.region}';
            SET s3_url_style='path';
            SET s3_use_ssl=true;
            ATTACH '{self._catalog_local}' AS pysus (READ_ONLY);
            USE pysus;
        """)

    def load(self):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import nest_asyncio

                nest_asyncio.apply()
            loop.run_until_complete(self.load_catalog())
        except RuntimeError:
            asyncio.run(self.load_catalog())

    async def load_catalog(self):
        async with httpx.AsyncClient(follow_redirects=True) as client:
            local_size = (
                self._catalog_local.stat().st_size
                if self._catalog_local.exists()
                else -1
            )
            r = await client.head(self.catalog_url)
            r.raise_for_status()
            remote_size = int(r.headers.get("content-length", 0))
            if remote_size != local_size:
                await self._download_catalog(client)

        if self._con is None:
            self._connect()

    def query(self, sql: str):
        if self._con is None:
            self._connect()
        return self._con.execute(sql).df()
