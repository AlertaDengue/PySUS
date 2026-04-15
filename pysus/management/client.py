import os
from collections.abc import Callable
from datetime import datetime
from logging import error
from pathlib import Path

from anyio import to_thread
from pysus.api.client import PySUS
from pysus.api.dadosgov.models import File as APIFile
from pysus.api.ducklake.catalog import (
    CatalogDataset,
    CatalogFile,
    ColumnDefinition,
    DatasetGroup,
    Origin,
)
from pysus.api.extensions import Parquet
from pysus.api.ftp.models import File as FTPFile
from pysus.api.models import BaseRemoteFile


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
        ducklake = await self.pysus.get_ducklake()
        await ducklake.login(self.access_key, self.secret_key)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if not exc_type:
                ducklake = self.pysus._ducklake
                if ducklake:
                    await ducklake._upload_catalog()
        except Exception as e:  # noqa
            error(e)
            pass
        finally:
            await self.pysus.__aexit__(exc_type, exc_val, exc_tb)

    async def upload(
        self,
        file: FTPFile | APIFile,
        callback: Callable[[int, int], None] | None = None,
    ) -> None:
        if not self.pysus._ducklake:
            raise ConnectionError("DuckLake is not connected")

        s3_key = (
            f"public/data/{file.client.name.lower()}"
            f"/{file.dataset.name.lower()}/{file.basename}"
        )

        with self.pysus._ducklake._Session() as session:
            dataset = self._get_or_create_dataset(session, file)

            existing = (
                session.query(CatalogFile)
                .filter(
                    CatalogFile.dataset_id == dataset.id,
                    CatalogFile.origin_path == str(file.path),
                )
                .first()
            )

            if not existing:
                existing = (
                    session.query(CatalogFile)
                    .filter(
                        CatalogFile.path == s3_key,
                        CatalogFile.dataset_id == dataset.id,
                    )
                    .first()
                )

            if not existing:
                existing = (
                    session.query(CatalogFile)
                    .filter(
                        CatalogFile.path
                        == str(Path(s3_key).with_suffix(".parquet")),
                        CatalogFile.dataset_id == dataset.id,
                    )
                    .first()
                )

            if existing and self._should_upload(file, existing):
                return

            group = self._get_or_create_group(session, file, dataset)
            cat_file = self._get_or_create_file(session, file, dataset, group)

        parquet_ext = await self.pysus.download_to_parquet(
            file=file, token=self.dadosgov_token, callback=callback
        )

        with self.pysus._ducklake._Session() as session:
            dataset = self._get_or_create_dataset(session, file)
            group = self._get_or_create_group(session, file, dataset)
            cat_file = self._get_or_create_file(session, file, dataset, group)

            existing_conflict = (
                session.query(CatalogFile)
                .filter(
                    CatalogFile.path == s3_key,
                    CatalogFile.dataset_id == dataset.id,
                )
                .first()
            )

            if existing_conflict:
                cat_file = existing_conflict
            else:
                cat_file = session.merge(cat_file)

            await self._upload_to_s3(parquet_ext.path, s3_key)

            cat_file.path = s3_key
            cat_file.size = parquet_ext.size
            cat_file.rows = parquet_ext.rows
            cat_file.modified = datetime.utcnow()
            cat_file.origin_modified = file.modify
            cat_file.columns = self._get_or_create_columns(
                session, dataset, parquet_ext
            )

            session.commit()

        parquet_ext.path.unlink()
        await self.pysus._delete_record(str(parquet_ext.path))

    async def _upload_to_s3(
        self,
        local_path: Path,
        s3_path: str,
        callback: Callable[[int], None] | None = None,
    ):
        def _do_upload():
            if not self.pysus._ducklake:
                raise ConnectionError("DuckLake not connected")
            self.pysus._ducklake._s3_client.upload_file(
                str(local_path),
                self.pysus._ducklake.bucket,
                s3_path,
                Callback=callback,
            )

        await to_thread.run_sync(_do_upload)

    def _should_upload(
        self,
        file: BaseRemoteFile,
        catalog_file: CatalogFile | None = None,
        force: bool = False,
    ) -> bool:
        if force:
            print(f"force=True, uploading {file.basename}")
            return True

        if catalog_file is None:
            print(f"no catalog record, uploading {file.basename}")
            return True

        if catalog_file.origin_modified is None:
            print(f"no origin_modified, uploading {file.basename}")
            return True

        file_mod = getattr(file, "modify", None)
        if file_mod is None:
            print(f"no file modify date, uploading {file.basename}")
            return True

        if file_mod > catalog_file.origin_modified:
            print(f"{catalog_file.origin_modified} newer than ({file_mod})")
            return True

        file_size = getattr(file, "size", None)
        if file_size and file_size != catalog_file.size:
            print(f"size differs: {file_size} != {catalog_file.size}")
            return True

        print(f"skipping {file.basename} - already up to date")
        return False

    def _get_or_create_dataset(
        self,
        session,
        file: BaseRemoteFile,
    ) -> CatalogDataset:
        ds_name = file.dataset.name.lower()
        ds = session.query(CatalogDataset).filter_by(name=ds_name).first()
        if not ds:
            is_ftp = file.client.name.lower() == "ftp"
            origin = Origin.FTP if is_ftp else Origin.API
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
                name=group_name,
                dataset=dataset,
                long_name=file.group.long_name,
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
                origin_path=str(file.path),
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
