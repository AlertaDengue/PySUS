"""High-level client for DuckLake S3-based public health dataset catalog.

Provides authentication, dataset discovery, and file download
capabilities backed by per-dataset DuckDB engines.
"""

import asyncio
from collections.abc import Callable
from pathlib import Path

from anyio import to_thread
from pydantic import SecretStr, PrivateAttr, Field
from pysus.api.models import BaseRemoteClient
from pysus.api.types import DUCKLAKE

from .catalog.orm.default import Dataset
from .catalog.adapters import DatasetAdapter, CatalogAdapter
from .models import DuckDataset, File
from .catalog.adapters import DuckLakeCredentials
from .functional import download_s3


class DuckLake(BaseRemoteClient):
    credentials: DuckLakeCredentials | None = None
    update_on_close: bool = Field(default=False, exclude=True)
    _datasets: list[DuckDataset] = PrivateAttr(default_factory=list)
    _catalog_adap: CatalogAdapter = PrivateAttr()

    def __init__(self, engine=None, update_on_close: bool = False, **data) -> None:
        super().__init__(**data)
        self.update_on_close = update_on_close
        self._catalog_adap = CatalogAdapter(
            engine=engine,
            credentials=self.credentials,
            update_on_close=self.update_on_close,
        )

    @property
    def name(self) -> str:
        return DUCKLAKE

    @property
    def long_name(self) -> str:
        return "PySUS s3 Client"

    @property
    def description(self) -> str:
        return ""

    async def datasets(self, **kwargs) -> list[DuckDataset]:
        def _fetch():
            with self._catalog_adap.get_session() as session:
                results = session.query(Dataset).all()
                session.expunge_all()
                return results

        duck_datasets: list[DuckDataset] = []

        async with self._catalog_adap:
            records = await to_thread.run_sync(_fetch)

            for rec in records:
                dataset_adapter = DatasetAdapter(
                    name=str(rec.name),
                    credentials=self.credentials,
                    update_on_close=self.update_on_close,
                )
                duck_datasets.append(
                    DuckDataset(
                        record=rec,
                        client=self,
                        adapter=dataset_adapter,
                        update_on_close=self.update_on_close,
                    )
                )

        self._datasets = duck_datasets
        return duck_datasets

    async def login(
        self,
        access_key: str,
        secret_key: str,
        **kwargs,
    ) -> None:
        self.credentials = DuckLakeCredentials(
            access_key=SecretStr(access_key),
            secret_key=SecretStr(secret_key),
        )
        self._catalog_adap.credentials = self.credentials
        await self._catalog_adap.connect(force=True)

    async def connect(self, force: bool = False) -> None:
        await self._catalog_adap.connect(force=force)

    async def close(self, update_catalog: bool | None = None) -> None:
        should_update = (
            self.update_on_close if update_catalog is None else update_catalog
        )

        for ds in self._datasets:
            await ds.close(update_catalog=should_update)

        await self._catalog_adap.close(update=should_update)

    async def download(
        self,
        file: File,
        output: Path,
        callback: Callable[[int, int], None] | None = None,
    ) -> Path:
        if not isinstance(file, File):
            raise ValueError("DuckLake File was not properly instantiated")

        access_key = (
            self.credentials.access_key.get_secret_value() if self.credentials else None
        )
        secret_key = (
            self.credentials.secret_key.get_secret_value() if self.credentials else None
        )

        await download_s3(
            remote_path=file.record.path,
            local_path=output,
            access_key=access_key,
            secret_key=secret_key,
            callback=callback,
        )
        return output

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close(update_catalog=None)

    def __del__(self) -> None:
        if not hasattr(self, "_catalog_adap"):
            return
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                loop.create_task(self.close(update_catalog=False))
        except RuntimeError:
            try:
                asyncio.run(self.close(update_catalog=False))
            except Exception:
                pass
        except Exception:
            pass


DuckDataset.model_rebuild(_types_namespace={"DuckLake": DuckLake})
