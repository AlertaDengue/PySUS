"""High-level client for DuckLake S3-based public health dataset catalog.

Provides authentication, dataset discovery, and file download
capabilities backed by per-dataset DuckDB engines.
"""

from collections.abc import Callable
from pathlib import Path

from anyio import to_thread
from pydantic import SecretStr, PrivateAttr
from pysus.api.models import BaseRemoteClient
from pysus.api.types import DUCKLAKE

from .catalog.orm.default import Dataset
from .catalog.adapters import DatasetAdapter, CatalogAdapter
from .models import DuckDataset, File
from .catalog.adapters import DuckLakeCredentials
from .functional import download_s3


class DuckLake(BaseRemoteClient):
    credentials: DuckLakeCredentials | None = None
    _datasets: list[DuckDataset] = PrivateAttr(default_factory=list)

    def __init__(self, engine=None, **data) -> None:
        super().__init__(**data)
        self.catalog_adap = CatalogAdapter(
            engine=engine,
            credentials=self.credentials,
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
        await self.catalog_adap.connect()

        def _fetch():
            with self.catalog_adap.get_session() as session:
                results = session.query(Dataset).all()
                session.expunge_all()
                return results

        records = await to_thread.run_sync(_fetch)

        duck_datasets: list[DuckDataset] = []
        for rec in records:
            dataset_adapter = DatasetAdapter(
                name=str(rec.name), credentials=self.credentials
            )
            duck_datasets.append(
                DuckDataset(record=rec, client=self, adapter=dataset_adapter)
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
        self.catalog_adap.credentials = self.credentials
        await self.catalog_adap.connect(force=True)

    async def connect(self, force: bool = False) -> None:
        await self.catalog_adap.connect(force=force)

    async def close(self, update_catalog: bool = False) -> None:
        for ds in self._datasets:
            await ds.close(update_catalog=update_catalog)

        await self.catalog_adap.close(update=update_catalog)
        self._datasets.clear()

    async def download(
        self,
        file: File,
        output: Path,
        callback: Callable[[int, int], None] | None = None,
    ) -> Path:
        if not isinstance(file, File):
            raise ValueError("FTP File was not properly instantiated")

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


DuckDataset.model_rebuild(_types_namespace={"DuckLake": DuckLake})
