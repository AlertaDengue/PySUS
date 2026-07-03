"""High-level client for DuckLake S3-based public health dataset catalog.

Provides authentication, dataset discovery, and file download
capabilities backed by per-dataset DuckDB engines.
"""

import asyncio
from collections.abc import Callable
from pathlib import Path

from anyio import to_thread
from pydantic import Field, PrivateAttr, SecretStr
from pysus.api.errors import AuthenticationError, ValidationError
from pysus.api.models import BaseRemoteClient, BaseRemoteFile
from pysus.api.types import DUCKLAKE

from .catalog.adapters import (
    CatalogAdapter,
    ColumnsAdapter,
    DatasetAdapter,
    DuckLakeCredentials,
)
from .catalog.orm.default import Dataset
from .functional import download_http
from .models import DuckDataset, File


class DuckLake(BaseRemoteClient):
    credentials: DuckLakeCredentials | None = None
    update_on_close: bool = Field(default=False, exclude=True)
    _datasets: list[DuckDataset] = PrivateAttr(default_factory=list)
    _catalog_adap: CatalogAdapter = PrivateAttr()
    _columns_adap: ColumnsAdapter = PrivateAttr()

    def __init__(
        self,
        engine=None,
        columns_engine=None,
        update_on_close: bool = False,
        **data,
    ) -> None:
        super().__init__(**data)
        self.update_on_close = update_on_close
        self._catalog_adap = CatalogAdapter(
            engine=engine,
            credentials=self.credentials,
            update_on_close=self.update_on_close,
        )
        self._columns_adap = ColumnsAdapter(
            engine=columns_engine,
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

    @property
    def catalog_path(self) -> Path:
        return self._catalog_adap.db_local

    @property
    def columns_path(self) -> Path:
        return self._columns_adap.db_local

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
                    dataset_id=int(rec.id),
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

    async def login(self, **kwargs) -> None:
        access_key = kwargs.get("access_key")
        secret_key = kwargs.get("secret_key")

        if not access_key or not secret_key:
            raise AuthenticationError(
                "DuckLake authentication requires 'access_key' and 'secret_key'"
            )

        self.credentials = DuckLakeCredentials(
            access_key=SecretStr(access_key),
            secret_key=SecretStr(secret_key),
        )
        self._catalog_adap.credentials = self.credentials
        self._columns_adap.credentials = self.credentials
        await self._catalog_adap.connect(force=True)
        await self._columns_adap.connect(force=True)

    async def connect(
        self,
        force: bool = False,
        callback: Callable[[int, int], None] | None = None,
    ) -> None:
        await self._catalog_adap.connect(force=force, callback=callback)
        await self._columns_adap.connect(force=force, callback=callback)

    async def close(self, update_catalog: bool | None = None) -> None:
        should_update = (
            self.update_on_close if update_catalog is None else update_catalog
        )

        for ds in self._datasets:
            await ds.close(update_catalog=should_update)

        await self._catalog_adap.close(update=should_update)
        await self._columns_adap.close(update=should_update)

    async def download(
        self,
        file: BaseRemoteFile,
        output: Path,
        callback: Callable[[int, int], None] | None = None,
    ) -> Path:
        if not isinstance(file, File):
            raise ValidationError("DuckLake File was not properly instantiated")

        await download_http(
            remote_path=file.record.path,
            local_path=output,
            callback=callback,
        )
        return output

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close(update_catalog=None)

    def __del__(self) -> None:
        if not hasattr(self, "_catalog_adap") or not hasattr(
            self, "_columns_adap"
        ):
            return
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                loop.create_task(self.close(update_catalog=False))
        except RuntimeError:
            try:
                asyncio.run(self.close(update_catalog=False))
            except Exception:  # noqa
                pass
        except Exception:  # noqa
            pass


DuckDataset.model_rebuild(_types_namespace={"DuckLake": DuckLake})
