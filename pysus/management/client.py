import asyncio
import os
from collections.abc import Callable
from logging import error
from pathlib import Path

from anyio import to_thread
from pysus.api.client import PySUS
from pysus.api.dadosgov.models import File as APIFile
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
            f"/{file.dataset.name.lower()}"
            f"/{file.path.with_suffix('.parquet').name}"
        )

        dataset_id = None
        group_id = None

        engine = self.pysus._ducklake._engine
        with engine.raw_connection() as conn:
            cursor = conn.cursor()
            try:
                dataset_name = file.dataset.name.lower()
                is_ftp = file.client.name.lower() == "ftp"

                cursor.execute(
                    f"SELECT id FROM pysus.datasets WHERE name = '{dataset_name}'"  # noqa
                )
                row = cursor.fetchone()

                if row:
                    dataset_id = row[0]
                    origin_val = "'FTP'" if is_ftp else "'API'"
                    cursor.execute(
                        f"UPDATE pysus.datasets SET origin = {origin_val} "
                        f"WHERE id = {dataset_id}"
                    )
                else:
                    cursor.execute("SELECT MAX(id) FROM pysus.datasets")
                    max_id = cursor.fetchone()[0]
                    dataset_id = (max_id or 0) + 1
                    origin_val = "'FTP'" if is_ftp else "'API'"
                    cursor.execute(
                        f"INSERT INTO pysus.datasets (id, name, "
                        f"long_name, origin) "
                        f"VALUES ({dataset_id}, '{dataset_name}', "
                        f"'{file.dataset.long_name}', {origin_val})"
                    )

                if file.group:
                    group_name = file.group.name
                    cursor.execute(
                        "SELECT id FROM pysus.dataset_groups "
                        f"WHERE name = '{group_name}' AND "
                        f"dataset_id = {dataset_id}"
                    )
                    row = cursor.fetchone()
                    if row:
                        group_id = row[0]
                    else:
                        cursor.execute(
                            "SELECT MAX(id) FROM pysus.dataset_groups",
                        )
                        max_id = cursor.fetchone()[0]
                        group_id = (max_id or 0) + 1
                        long_name = file.dataset.group_definitions.get(
                            group_name.upper(), group_name
                        )
                        cursor.execute(
                            f"INSERT INTO pysus.dataset_groups "
                            f"(id, dataset_id, name, long_name) "
                            f"VALUES ({group_id}, {dataset_id}, "
                            f"'{group_name}', '{long_name}')"
                        )

                group_val = "NULL" if group_id is None else str(group_id)

                cursor.execute(
                    f"SELECT id, group_id FROM pysus.files WHERE path = '{s3_key}'"  # noqa
                )
                row = cursor.fetchone()

                if row:
                    file_id, db_group_id = row

                    group_mismatch = db_group_id != group_id
                    should_upload = self._should_upload_raw(
                        cursor,
                        file_id,
                        file,
                    )

                    if not should_upload and not group_mismatch:
                        return

                    cursor.execute(
                        f"DELETE FROM pysus.file_columns WHERE file_id = {file_id}"  # noqa
                    )
                    cursor.execute(
                        f"DELETE FROM pysus.files WHERE id = {file_id}",
                    )
                else:
                    cursor.execute("SELECT MAX(id) FROM pysus.files")
                    max_id = cursor.fetchone()[0]
                    file_id = (max_id or 0) + 1

                parquet_ext = await self._download_with_retry(file, callback)
                await self._upload_to_s3(parquet_ext.path, s3_key)

                year_val = "NULL" if file.year is None else file.year
                month_val = "NULL" if file.month is None else file.month
                state_val = "NULL" if file.state is None else f"'{file.state}'"

                cursor.execute(
                    f"INSERT INTO pysus.files (id, dataset_id, "
                    f"group_id, path, size, rows, "
                    f"modified, origin_modified, origin_path, year, "
                    f"month, state) "
                    f"VALUES ({file_id}, {dataset_id}, {group_val}, "
                    f"'{s3_key}', {parquet_ext.size}, "
                    f"{parquet_ext.rows}, CURRENT_TIMESTAMP, "
                    f"'{file.modify}', '{file.path}', "
                    f"{year_val}, {month_val}, {state_val})"
                )

                new_columns = self._get_or_create_columns_raw(
                    cursor, parquet_ext, dataset_id
                )

                for col in new_columns:
                    cursor.execute(
                        f"INSERT INTO pysus.file_columns "
                        f"(file_id, column_id) VALUES ({file_id}, {col})"
                    )

                conn.commit()
                cursor.execute("CHECKPOINT")

                if parquet_ext.path.exists():
                    parquet_ext.path.unlink()
                await self.pysus._delete_record(str(parquet_ext.path))

            except Exception:  # noqa
                try:
                    conn.rollback()
                except Exception:  # noqa
                    pass
                raise

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
            f"SELECT origin_modified FROM pysus.files WHERE id = {file_id}",
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

        return file_mod > origin_modified

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
                f"WHERE name = '{col_name}' AND dataset_id = {dataset_id}"
            )
            existing = cursor.fetchone()

            if existing:
                result.append(existing[0])
            else:
                cursor.execute("SELECT MAX(id) FROM pysus.dataset_columns")
                max_id = cursor.fetchone()[0]
                new_id = (max_id or 0) + 1
                cursor.execute(
                    "INSERT INTO pysus.dataset_columns "
                    "(id, dataset_id, name, type, nullable) "
                    f"VALUES ({new_id}, {dataset_id}, '{col_name}', "
                    f"'{sql_type}', true)"
                )
                result.append(new_id)

        return result
