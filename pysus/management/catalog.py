"""Catalog persistence: write file metadata into the DuckLake duckdb catalogs.

The writer uses parameterized upserts against the DuckDB engines managed by
the DuckLake adapters. It is idempotent (keyed on the S3 ``path``), keeps
the legacy columns in sync, and adds a few management columns
(``origin``, ``format``, ``sha256``) when missing, so the catalog stays the
single source of truth for every file tracked by the workflow.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pysus.api.errors import CatalogError

if TYPE_CHECKING:  # pragma: no cover
    from pysus.api.ducklake.client import DuckLake

_ARROW_TO_SQL = {
    "int64": "BIGINT",
    "int32": "INTEGER",
    "int16": "INTEGER",
    "int8": "INTEGER",
    "uint64": "BIGINT",
    "uint32": "BIGINT",
    "uint16": "INTEGER",
    "uint8": "INTEGER",
    "double": "DOUBLE",
    "float": "FLOAT",
    "bool": "BOOLEAN",
    "timestamp[us]": "TIMESTAMP",
    "timestamp[ns]": "TIMESTAMP",
    "date32[day]": "DATE",
    "string": "VARCHAR",
    "large_string": "VARCHAR",
    "binary": "BLOB",
}

_FILES_BASE_COLUMNS = (
    "dataset_id",
    "group_id",
    "path",
    "size",
    "rows",
    "modified",
    "origin_modified",
    "origin_size",
    "origin_path",
    "year",
    "month",
    "state",
)


def sha256_of(path: Path) -> str:
    """Compute the sha256 digest of a local file."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


class CatalogWriter:
    """Upsert dataset/group/file/column metadata into the DuckLake catalogs."""

    def __init__(self, ducklake: DuckLake):
        self.ducklake = ducklake

    # ------------------------------------------------------------------
    # low-level plumbing
    # ------------------------------------------------------------------
    @property
    def _catalog_engine(self):
        engine = self.ducklake._catalog_adap._engine
        if engine is None:
            raise CatalogError("DuckLake catalog engine is not initialized")
        return engine

    @property
    def _columns_engine(self):
        engine = self.ducklake._columns_adap._engine
        if engine is None:
            raise CatalogError("DuckLake columns engine is not initialized")
        return engine

    def _has_column(self, cursor, table: str, column: str) -> bool:
        cursor.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'pysus' AND table_name = ? "
            "AND column_name = ?",
            (table, column),
        )
        return cursor.fetchone() is not None

    def _ensure_column(
        self, cursor, table: str, column: str, definition: str
    ) -> None:
        if self._has_column(cursor, table, column):
            return
        cursor.execute(
            f'ALTER TABLE pysus.{table} ADD COLUMN "{column}" {definition}'
        )

    def _ensure_management_columns(self, catalog_cursor) -> None:
        self._ensure_column(catalog_cursor, "files", "origin", "VARCHAR")
        self._ensure_column(catalog_cursor, "files", "format", "VARCHAR")

    # ------------------------------------------------------------------
    # datasets & groups
    # ------------------------------------------------------------------
    def ensure_dataset(
        self,
        cursor,
        name: str,
        long_name: str,
        description: str | None = None,
    ) -> int:
        """Return the dataset id, creating the row if needed."""
        name = name.strip().lower()
        cursor.execute(
            "SELECT id, long_name, description FROM pysus.datasets "
            "WHERE name = ?",
            (name,),
        )
        row = cursor.fetchone()
        if row:
            dataset_id = row[0]
            if row[1] != long_name or row[2] != description:
                cursor.execute(
                    "UPDATE pysus.datasets SET long_name = ?, "
                    "description = ? WHERE id = ?",
                    (long_name, description, dataset_id),
                )
            return int(dataset_id)

        cursor.execute("SELECT MAX(id) FROM pysus.datasets")
        max_row = cursor.fetchone()
        dataset_id = (max_row[0] or 0) + 1
        cursor.execute(
            "INSERT INTO pysus.datasets (id, name, long_name, description) "
            "VALUES (?, ?, ?, ?)",
            (dataset_id, name, long_name, description),
        )
        return int(dataset_id)

    def ensure_group(
        self,
        cursor,
        dataset_id: int,
        name: str | None,
        long_name: str | None = None,
        description: str | None = None,
    ) -> int | None:
        """Return the group id for (dataset, name), creating it if needed."""
        if not name:
            return None

        name = name.strip().upper()
        cursor.execute(
            "SELECT id, long_name, description FROM pysus.dataset_groups "
            "WHERE dataset_id = ? AND name = ?",
            (dataset_id, name),
        )
        row = cursor.fetchone()
        if row:
            group_id = row[0]
            if row[1] != long_name or row[2] != description:
                cursor.execute(
                    "UPDATE pysus.dataset_groups SET long_name = ?, "
                    "description = ? WHERE id = ?",
                    (long_name, description, group_id),
                )
            return int(group_id)

        cursor.execute("SELECT MAX(id) FROM pysus.dataset_groups")
        max_row = cursor.fetchone()
        group_id = (max_row[0] or 0) + 1
        cursor.execute(
            "INSERT INTO pysus.dataset_groups (id, dataset_id, name, "
            "long_name, description) VALUES (?, ?, ?, ?, ?)",
            (group_id, dataset_id, name, long_name, description),
        )
        return int(group_id)

    # ------------------------------------------------------------------
    # files
    # ------------------------------------------------------------------
    def get_file(self, cursor, path: str) -> tuple[int, datetime | None] | None:
        """Return ``(id, origin_modified)`` for the S3 *path*, if present."""
        cursor.execute(
            "SELECT id, origin_modified FROM pysus.files WHERE path = ?",
            (path,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return int(row[0]), row[1]

    def delete_file(self, cursor, file_id: int) -> None:
        cursor.execute(
            "DELETE FROM pysus.file_columns WHERE file_id = ?", (file_id,)
        )
        cursor.execute("DELETE FROM pysus.files WHERE id = ?", (file_id,))

    def upsert_file(
        self,
        cursor,
        *,
        dataset_id: int,
        group_id: int | None,
        path: str,
        size: int,
        rows: int,
        modified: datetime | None,
        origin_modified: datetime | None,
        origin_size: int,
        origin_path: str,
        year: int | None,
        month: int | None,
        state: str | None,
        origin: str | None = None,
        format: str | None = None,
        sha256: str | None = None,
        file_type: str | None = None,
    ) -> tuple[int, bool]:
        """Insert or update the file row keyed on S3 *path*.

        Returns ``(file_id, created)``.
        """
        existing = self.get_file(cursor, path)
        if existing:
            file_id, _ = existing
            sets = [
                "size = ?",
                "rows = ?",
                "modified = ?",
                "origin_modified = ?",
                "origin_size = ?",
                "origin_path = ?",
                "year = ?",
                "month = ?",
                "state = ?",
            ]
            update_values: list[Any] = [
                size,
                rows,
                modified or datetime.now(),
                origin_modified,
                origin_size,
                origin_path,
                year,
                month,
                state,
            ]
            if origin is not None:
                sets.append("origin = ?")
                update_values.append(origin)
            if format is not None:
                sets.append("format = ?")
                update_values.append(format)
            if sha256 is not None:
                sets.append("sha256 = ?")
                update_values.append(sha256)
            if file_type is not None:
                sets.append("type = ?")
                update_values.append(file_type)
            update_values.append(file_id)
            cursor.execute(
                f"UPDATE pysus.files SET {', '.join(sets)} WHERE id = ?",
                update_values,
            )
            return file_id, False

        cursor.execute("SELECT MAX(id) FROM pysus.files")
        max_row = cursor.fetchone()
        file_id = (max_row[0] or 0) + 1
        columns = ["id", *_FILES_BASE_COLUMNS]
        values: list[Any] = [
            file_id,
            dataset_id,
            group_id,
            path,
            size,
            rows,
            modified or datetime.now(),
            origin_modified,
            origin_size,
            origin_path,
            year,
            month,
            state,
        ]
        if origin is not None:
            columns.append("origin")
            values.append(origin)
        if format is not None:
            columns.append("format")
            values.append(format)
        if sha256 is not None:
            columns.append("sha256")
            values.append(sha256)
        if file_type is not None:
            columns.append("type")
            values.append(file_type)
        placeholders = ", ".join("?" for _ in columns)
        cursor.execute(
            f"INSERT INTO pysus.files ({', '.join(columns)}) "
            f"VALUES ({placeholders})",
            values,
        )
        return file_id, True

    # ------------------------------------------------------------------
    # columns
    # ------------------------------------------------------------------
    def link_columns(
        self,
        dataset_cursor,
        columns_cursor,
        file_id: int,
        schema,
        dataset_id: int,
    ) -> None:
        """Get-or-create column definitions for *schema* and link them.

        Column definitions live in the columns catalog; the
        ``file_columns`` links live next to the files in the per-dataset
        catalog.
        """
        column_ids: list[int] = []
        for col_name in schema.names:
            arrow_type = str(schema.field(col_name).type)
            sql_type = _ARROW_TO_SQL.get(arrow_type, "VARCHAR")

            columns_cursor.execute(
                "SELECT id FROM pysus.dataset_columns "
                "WHERE dataset_id = ? AND name = ?",
                (dataset_id, col_name),
            )
            existing = columns_cursor.fetchone()
            if existing:
                column_ids.append(existing[0])
                continue

            columns_cursor.execute("SELECT MAX(id) FROM pysus.dataset_columns")
            max_row = columns_cursor.fetchone()
            new_id = (max_row[0] or 0) + 1
            columns_cursor.execute(
                "INSERT INTO pysus.dataset_columns (id, dataset_id, name, "
                "type, nullable) VALUES (?, ?, ?, ?, true)",
                (new_id, dataset_id, col_name, sql_type),
            )
            column_ids.append(new_id)

        dataset_cursor.execute(
            "DELETE FROM pysus.file_columns WHERE file_id = ?", (file_id,)
        )
        for column_id in column_ids:
            dataset_cursor.execute(
                "INSERT INTO pysus.file_columns (file_id, column_id) "
                "VALUES (?, ?)",
                (file_id, column_id),
            )
