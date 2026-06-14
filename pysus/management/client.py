import asyncio
import os
from collections.abc import Callable
from logging import error
from pathlib import Path

from pysus.api.client import PySUS
from pysus.api.dadosgov.models import File as APIFile
from pysus.api.ducklake.functional import upload_s3
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

        if not self.access_key or not self.secret_key:
            raise ValueError("s3 credentials are needed")

    async def __aenter__(self):
        await self.pysus.__aenter__()
        ducklake = await self.pysus.get_ducklake()
        await ducklake.login(
            access_key=self.access_key, secret_key=self.secret_key
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if not exc_type:
                ducklake = self.pysus._ducklake
                if ducklake:
                    await ducklake.close(update_catalog=True)
        finally:
            await self.pysus.__aexit__(exc_type, exc_val, exc_tb)

    async def upload(
        self,
        file: FTPFile | APIFile,
        callback: Callable[[int, int], None] | None = None,
    ) -> None:
        if not self.pysus._ducklake:
            raise ConnectionError("DuckLake is not connected")

        remote_file_path = Path(file.path)
        s3_key = (
            f"public/data/{file.client.name.lower()}"
            f"/{file.dataset.name.lower()}"
            f"/{remote_file_path.with_suffix('.parquet').name}"
        )

        dataset_id = None
        group_id = None

        catalog_engine = self.pysus._ducklake._catalog_adap._engine
        columns_engine = self.pysus._ducklake._columns_adap._engine

        if not catalog_engine or not columns_engine:
            raise ConnectionError(
                "DuckLake database engines are not initialized"
            )

        catalog_conn = catalog_engine.raw_connection()
        columns_conn = columns_engine.raw_connection()

        with catalog_conn, columns_conn:
            catalog_cursor = catalog_conn.cursor()
            columns_cursor = columns_conn.cursor()

            try:
                dataset_name = file.dataset.name.lower()
                is_ftp = file.client.name.lower() == "ftp"

                catalog_cursor.execute(
                    "SELECT id FROM pysus.datasets WHERE name = ?",
                    (dataset_name,),
                )
                row = catalog_cursor.fetchone()

                if row:
                    dataset_id = row[0]
                    origin_val = "'FTP'" if is_ftp else "'API'"
                    catalog_cursor.execute(
                        f"UPDATE pysus.datasets SET origin = {origin_val} "
                        f"WHERE id = {dataset_id}"
                    )
                else:
                    catalog_cursor.execute("SELECT MAX(id) FROM pysus.datasets")
                    max_id_row = catalog_cursor.fetchone()
                    max_id = max_id_row[0] if max_id_row else None
                    dataset_id = (max_id or 0) + 1
                    origin_val = "'FTP'" if is_ftp else "'API'"
                    catalog_cursor.execute(
                        f"INSERT INTO pysus.datasets (id, name, long_name, "
                        f"origin) VALUES ({dataset_id}, '{dataset_name}', "
                        f"'{file.dataset.long_name}', {origin_val})"
                    )

                if file.group:
                    group_name = file.group.name
                    catalog_cursor.execute(
                        "SELECT id FROM pysus.dataset_groups "
                        "WHERE name = ? AND dataset_id = ?",
                        (group_name, dataset_id),
                    )
                    row = catalog_cursor.fetchone()
                    if row:
                        group_id = row[0]
                    else:
                        catalog_cursor.execute(
                            "SELECT MAX(id) FROM pysus.dataset_groups"
                        )
                        max_id_row = catalog_cursor.fetchone()
                        max_id = max_id_row[0] if max_id_row else None
                        group_id = (max_id or 0) + 1
                        long_name = file.dataset.group_definitions.get(
                            group_name.upper(), group_name
                        )
                        catalog_cursor.execute(
                            f"INSERT INTO pysus.dataset_groups (id, "
                            f"dataset_id, name, long_name) VALUES ({group_id},"
                            f" {dataset_id}, '{group_name}', '{long_name}')"
                        )

                group_val = "NULL" if group_id is None else str(group_id)

                catalog_cursor.execute(
                    "SELECT id, group_id FROM pysus.files WHERE path = ?",
                    (s3_key,),
                )
                row = catalog_cursor.fetchone()

                if row:
                    file_id, db_group_id = row
                    group_mismatch = db_group_id != group_id
                    should_upload = self._should_upload_raw(
                        catalog_cursor,
                        file_id,
                        file,
                    )

                    if not should_upload and not group_mismatch:
                        return

                    columns_cursor.execute(
                        "DELETE FROM pysus.file_columns WHERE file_id = ?",
                        (file_id,),
                    )
                    catalog_cursor.execute(
                        "DELETE FROM pysus.files WHERE id = ?",
                        (file_id,),
                    )
                else:
                    catalog_cursor.execute("SELECT MAX(id) FROM pysus.files")
                    max_id_row = catalog_cursor.fetchone()
                    max_id = max_id_row[0] if max_id_row else None
                    file_id = (max_id or 0) + 1

                parquet_ext = await self._download_with_retry(file, callback)
                await self._upload_to_s3(parquet_ext.path, s3_key)

                year_val = "NULL" if file.year is None else str(file.year)
                month_val = "NULL" if file.month is None else str(file.month)
                state_val = "NULL" if file.state is None else f"'{file.state}'"

                catalog_cursor.execute(
                    f"INSERT INTO pysus.files (id, dataset_id, group_id, "
                    f"path, size, rows, modified, origin_modified, "
                    f"origin_path, year, month, state) VALUES ({file_id}, "
                    f"{dataset_id}, {group_val}, '{s3_key}', "
                    f"{parquet_ext.size}, {parquet_ext.rows}, "
                    f"CURRENT_TIMESTAMP, '{file.modify}', '{file.path}', "
                    f"{year_val}, {month_val}, {state_val})"
                )

                new_columns = self._get_or_create_columns_raw(
                    columns_cursor, parquet_ext, dataset_id
                )

                for col in new_columns:
                    columns_cursor.execute(
                        f"INSERT INTO pysus.file_columns (file_id, column_id) "
                        f"VALUES ({file_id}, {col})"
                    )

                catalog_conn.commit()
                columns_conn.commit()

                catalog_cursor.execute("CHECKPOINT")
                columns_cursor.execute("CHECKPOINT")

                if parquet_ext.path.exists():
                    parquet_ext.path.unlink()
                await self.pysus._delete_record(str(parquet_ext.path))

            except BaseException as rollback_err:  # noqa
                try:
                    catalog_conn.rollback()
                except Exception as inner_err:  # noqa
                    error(f"Catalog rollback failed: {inner_err}")
                try:
                    columns_conn.rollback()
                except Exception as inner_err:  # noqa
                    error(f"Columns rollback failed: {inner_err}")
                raise rollback_err

    async def _upload_to_s3(
        self,
        local_path: Path,
        s3_path: str,
        callback: Callable[[int, int], None] | None = None,
    ):
        await upload_s3(
            local_path=local_path,
            access_key=str(self.access_key),
            secret_key=str(self.secret_key),
            remote_path=s3_path,
            callback=callback,
        )

    async def _download_with_retry(
        self,
        file: FTPFile | APIFile,
        callback: Callable[[int, int], None] | None = None,
        max_retries: int = 3,
    ) -> Parquet:
        errors = (ConnectionResetError, ConnectionRefusedError, TimeoutError)
        last_error = None

        for attempt in range(max_retries):
            try:
                return await self.pysus.download_to_parquet(
                    file=file,
                    token=self.dadosgov_token,
                    callback=callback,
                )
            except errors as e:
                last_error = e
                wait_time = 2**attempt
                error(
                    f"Download attempt {attempt + 1}/{max_retries} failed "
                    f"for {file.basename}: {e}. Retrying in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)

        raise RuntimeError(
            f"Failed to download {file.basename} after {max_retries} "
            f"attempts: {last_error}"
        ) from last_error

    def _should_upload_raw(
        self,
        cursor,
        file_id: int,
        file: BaseRemoteFile,
        force: bool = False,
    ) -> bool:
        if force:
            return True

        cursor.execute(
            "SELECT origin_modified FROM pysus.files WHERE id = ?",
            (file_id,),
        )
        row = cursor.fetchone()
        if not row:
            return True

        origin_modified = row[0]
        if origin_modified is None:
            return True

        file_mod = getattr(file, "modify", None)
        if file_mod is None:
            return True

        return str(file_mod) > str(origin_modified)

    def _get_or_create_columns_raw(
        self, cursor, file: Parquet, dataset_id: int
    ) -> list[int]:
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

        result = []
        for col_name in schema.names:
            field = schema.field(col_name)
            arrow_type = str(field.type)
            sql_type = type_map.get(arrow_type, "VARCHAR")

            cursor.execute(
                "SELECT id FROM pysus.dataset_columns "
                "WHERE name = ? AND dataset_id = ?",
                (col_name, dataset_id),
            )
            existing = cursor.fetchone()

            if existing:
                result.append(existing[0])
            else:
                cursor.execute("SELECT MAX(id) FROM pysus.dataset_columns")
                max_id_row = cursor.fetchone()
                max_id = max_id_row[0] if max_id_row else None
                new_id = (max_id or 0) + 1
                cursor.execute(
                    "INSERT INTO pysus.dataset_columns (id, dataset_id, name, "
                    "type, nullable) VALUES (?, ?, ?, ?, true)",
                    (new_id, dataset_id, col_name, sql_type),
                )
                result.append(new_id)

        return result
