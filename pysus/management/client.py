import os
from datetime import datetime
from logging import error
from typing import Callable
from pathlib import Path

import anyio
from pysus.api.client import PySUS
from pysus.api.models import BaseRemoteFile
from pysus.api.ducklake.catalog import (
    CatalogFile,
    CatalogDataset,
    DatasetGroup,
    ColumnDefinition,
    Origin,
)
from pysus.api.ftp.models import File as FTPFile
from pysus.api.dadosgov.models import File as APIFile
from pysus.api.extensions import Parquet


class CatalogManager:
    def __init__(
        self,
        access_key: str | None = None,
        secret_key: str | None = None,
        dadosgov_token: str | None = None,
    ):
        self.pysus = PySUS()
        self.access_key = access_key or os.getenv("ACCESS_KEY")
        self.secret_key = secret_key or os.getenv("SECRET_KEY")
        self.dadosgov_token = dadosgov_token or os.getenv("DADOSGOV_TOKEN")

        if not access_key or not secret_key:
            raise ValueError("s3 credentials are needed")

    async def __aenter__(self):
        await self.pysus.__aenter__()
        await self.pysus._ducklake.login(self.access_key, self.secret_key)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if not exc_type:
                await self.pysus._ducklake._upload_catalog()
        except Exception as e:
            error(e)
            pass
        finally:
            await self.pysus.__aexit__(exc_type, exc_val, exc_tb)

    async def upload(
        self,
        file: FTPFile | APIFile,
        callback: Callable[[int, int], None] = None,
    ) -> None:
        with self.pysus._ducklake._Session() as session:
            dataset = self._get_or_create_dataset(session, file)
            group = self._get_or_create_group(session, file, dataset)
            cat_file = self._get_or_create_file(session, file, dataset, group)

            if not self._should_upload(file, cat_file):
                return

            session.commit()

        parquet_ext = await self.pysus.download_to_parquet(
            file=file, token=self.dadosgov_token, callback=callback
        )

        s3_key = (
            f"public/data/{file.client.name.lower()}"
            + f"/{file.dataset.name.lower()}/{parquet_ext.path.name}"
        )

        await self._upload_to_s3(parquet_ext.path, s3_key)

        with self.pysus._ducklake._Session() as session:
            current_dataset = self._get_or_create_dataset(session, file)
            current_group = self._get_or_create_group(session, file, current_dataset)

            cat_file = self._get_or_create_file(
                session, file, current_dataset, current_group
            )

            cat_file.path = s3_key
            cat_file.size = parquet_ext.size
            cat_file.rows = parquet_ext.rows
            cat_file.modified = datetime.utcnow()
            cat_file.origin_modified = file.modify
            cat_file.columns = self._get_or_create_columns(
                session, current_dataset, parquet_ext
            )

            session.commit()

        parquet_ext.path.unlink()
        await self.pysus._delete_record(str(parquet_ext.path))

    async def _upload_to_s3(
        self,
        local_path: Path,
        s3_path: str,
        callback: Callable[[int], None] = None,
    ):
        def _do_upload():
            self.pysus._ducklake._s3_client.upload_file(
                str(local_path),
                self.pysus._ducklake.bucket,
                s3_path,
                Callback=callback,
            )

        await anyio.to_thread.run_sync(_do_upload)

    def _should_upload(
        self,
        file: BaseRemoteFile,
        catalog_file: CatalogFile | None = None,
    ) -> bool:
        if catalog_file is None or catalog_file.origin_modified is None:
            return True

        return file.modify > catalog_file.origin_modified

    def _get_or_create_dataset(
        self,
        session,
        file: BaseRemoteFile,
        callback: Callable = None,
    ) -> CatalogDataset:
        ds_name = file.dataset.name.lower()
        ds = session.query(CatalogDataset).filter_by(name=ds_name).first()
        if not ds:
            origin = Origin.FTP if file.client.name.lower() == "ftp" else Origin.API
            ds = CatalogDataset(
                name=ds_name, long_name=file.dataset.long_name, origin=origin
            )
            session.add(ds)
            session.flush()
        return ds

    def _get_or_create_group(
        self,
        session,
        file: BaseRemoteFile,
        dataset: CatalogDataset,
        callback: Callable = None,
    ) -> DatasetGroup | None:
        if file.group is None:
            return None

        group_name = file.group.name
        group = (
            session.query(DatasetGroup)
            .filter_by(name=group_name, dataset_id=dataset.id)
            .first()
        )

        if not group:
            group = DatasetGroup(
                name=group_name, dataset=dataset, long_name=file.group.long_name
            )
            session.add(group)
            session.flush()
        return group

    def _get_or_create_file(
        self,
        session,
        file: BaseRemoteFile,
        dataset: CatalogDataset,
        group: DatasetGroup | None = None,
        callback: Callable = None,
    ) -> CatalogFile:
        query = session.query(CatalogFile).filter(
            CatalogFile.dataset_id == dataset.id,
            CatalogFile.group_id == (group.id if group else None),
            CatalogFile.year == file.year,
            CatalogFile.month == file.month,
            CatalogFile.state == file.state,
        )

        cat_file = query.first()

        if not cat_file:
            cat_file = CatalogFile(
                dataset=dataset,
                group=group,
                path=f"pending/{file.basename}",
                size=0,
                rows=0,
                modified=datetime.min,
                year=file.year,
                month=file.month,
                state=file.state,
            )
            session.add(cat_file)
            session.flush()

        return cat_file

    def _get_or_create_columns(
        self, session, dataset: CatalogDataset, file: Parquet
    ) -> list[ColumnDefinition]:
        existing_cols = {c.name: c for c in dataset.columns}
        result = []

        schema = file.schema

        type_map = {
            "int64": "BIGINT",
            "int32": "INTEGER",
            "double": "DOUBLE",
            "float": "FLOAT",
            "bool": "BOOLEAN",
            "timestamp[us]": "TIMESTAMP",
            "string": "VARCHAR",
            "binary": "BLOB",
        }

        for col_name in schema.names:
            field = schema.field(col_name)
            arrow_type = str(field.type)
            sql_type = type_map.get(arrow_type, "VARCHAR")

            if col_name not in existing_cols:
                new_col = ColumnDefinition(
                    name=col_name,
                    dataset=dataset,
                    type=sql_type,
                )
                session.add(new_col)
                existing_cols[col_name] = new_col
            else:
                if existing_cols[col_name].type != sql_type:
                    existing_cols[col_name].type = sql_type

            result.append(existing_cols[col_name])

        return result
