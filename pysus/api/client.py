"""Main orchestrator for the PySUS data pipeline.

Manages file downloads, local state tracking, catalog attachment,
Parquet conversion, and query execution across multiple backends.
"""

import enum
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import anyio
import duckdb
import pandas as pd
from duckdb import func
from pysus import CACHEPATH
from pysus.api.types import Origin
from sqlalchemy import DateTime, Enum, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from sqlalchemy.pool import NullPool

from .dadosgov import DadosGovClient
from .ducklake.client import DuckLake
from .extensions import Parquet
from .ftp import FTPClient
from .models import BaseLocalFile, BaseRemoteFile

if TYPE_CHECKING:  # pragma: no cover
    from duckdb import DuckDBPyConnection


class Base(DeclarativeBase):
    """Base declarative class for SQLAlchemy ORM models."""


class DownloadStatus(enum.Enum):
    """Download status values tracked for each local file."""

    PENDING = "pending"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"
    MISSING = "missing"


class LocalFileState(Base):
    """ORM model tracking the state of a downloaded local file."""

    __tablename__ = "local_file_state"
    path: Mapped[str] = mapped_column(String, primary_key=True)
    remote_path: Mapped[str] = mapped_column(String, nullable=False)
    client_name: Mapped[str] = mapped_column(String, nullable=False)

    year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    month: Mapped[int | None] = mapped_column(Integer, nullable=True)
    state: Mapped[str | None] = mapped_column(String, nullable=True)
    group: Mapped[str | None] = mapped_column(String, nullable=True)

    status: Mapped[DownloadStatus] = mapped_column(
        Enum(DownloadStatus),
        default=DownloadStatus.PENDING,
    )
    sha256: Mapped[str | None] = mapped_column(String, nullable=True)
    last_synced: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
    )


class PySUS:
    """Central orchestrator for downloading and querying PySUS datasets."""

    def __init__(self, db_path: Path = CACHEPATH / "config.db"):
        """Initialize the PySUS orchestrator.

        Creates a SQLAlchemy engine backed by DuckDB, initializes the
        schema, and sets up the session factory.

        Parameters
        ----------
        db_path : Path, optional
            Path to the DuckDB database file. Defaults to
            ``CACHEPATH / "config.db"``.
        """

        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self.cachepath = db_path.parent
        self.engine = create_engine(
            f"duckdb:///{db_path.resolve().as_posix()}",
            poolclass=NullPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

        self._ducklake: DuckLake | None = None
        self._ftp: FTPClient | None = None
        self._dadosgov: DadosGovClient | None = None

    async def __aenter__(self):
        """Set up DuckLake catalog and return self as async context manager."""

        self._ducklake = DuckLake()
        await self._ducklake.connect()
        self._attach_client_catalog(
            "ducklake",
            str(self._ducklake.catalog_path),
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up all client connections and dispose of the engine."""

        if self._ducklake:
            await self._ducklake.close()
        if self._ftp:
            await self._ftp.close()
        if self._dadosgov:
            await self._dadosgov.close()
        self.engine.dispose()

    async def get_ducklake(self) -> DuckLake:
        """Return the DuckLake client, initializing it lazily if needed."""

        if self._ducklake is None:
            self._ducklake = DuckLake()
            await self._ducklake.connect()
            self._attach_client_catalog(
                "ducklake",
                str(self._ducklake.catalog_path),
            )
        return self._ducklake

    async def get_dadosgov(self, access_token: str | None) -> DadosGovClient:
        """Return the DadosGov client, connecting lazily if needed."""

        if self._dadosgov is None:
            self._dadosgov = DadosGovClient()
            await self._dadosgov.connect(token=access_token)
        return self._dadosgov

    async def get_ftp(self) -> FTPClient:
        """Return the FTP client, connecting lazily if needed."""

        if self._ftp is None:
            self._ftp = FTPClient()
            await self._ftp.connect()
        return self._ftp

    async def get_local_file(
        self,
        file: BaseRemoteFile,
    ) -> BaseLocalFile | None:
        """Look up a previously downloaded file by its remote path."""

        from pysus.api.extensions import ExtensionFactory

        client_name = file.client.name.lower()
        remote_path = file.path

        with self.Session() as session:
            records = (
                session.query(LocalFileState)
                .filter_by(
                    remote_path=str(remote_path),
                    client_name=str(client_name),
                    status=DownloadStatus.COMPLETED,
                )
                .all()
            )

            if not records:
                return None

            parquet_version = next(
                (r for r in records if str(r.path).endswith(".parquet")), None
            )
            record = parquet_version or records[0]

            return await ExtensionFactory.instantiate(str(record.path))

    def _attach_client_catalog(self, name: str, path: str):
        """Attach an external DuckDB catalog to the engine if not attached."""

        abs_path = str(Path(path).absolute())
        with self.engine.connect() as conn:
            q = "SELECT database_name FROM duckdb_databases() WHERE path = ?"
            existing = conn.exec_driver_sql(q, (abs_path,)).fetchone()

            if not existing:
                conn.exec_driver_sql(
                    f"ATTACH '{abs_path}' AS {name} (READ_ONLY)",
                )

    def _get_dest_path(self, file: BaseRemoteFile) -> Path:
        """Build the local filesystem path for a given remote file."""

        client_name = file.client.name.lower()
        dataset_name = file.dataset.name.lower()

        group_name = ""
        if hasattr(file, "group") and file.group:
            group_name = getattr(file.group, "name", "")

        base_dir = self.cachepath / "downloads" / client_name / dataset_name

        if group_name:
            return base_dir / group_name / file.basename

        return base_dir / file.basename

    async def _update_state(
        self,
        local_path: Path,
        remote_path: str,
        client_name: str,
        status: DownloadStatus,
        year: int | None = None,
        month: int | None = None,
        state: str | None = None,
        group: str | None = None,
    ):
        """Create or update the LocalFileState record for a file."""

        with self.Session() as session:
            record = (
                session.query(LocalFileState)
                .filter_by(
                    path=str(local_path),
                )
                .first()
            )
            if not record:
                record = LocalFileState(
                    path=str(local_path),
                    remote_path=str(remote_path),
                    client_name=client_name,
                    year=year,
                    month=month,
                    state=state,
                    group=group,
                )
                session.add(record)

            record.status = status
            record.last_synced = datetime.now(timezone.utc).replace(tzinfo=None)
            session.commit()

    async def download(
        self,
        file: BaseRemoteFile,
        token: str | None = None,
        callback: Callable | None = None,
        timeout: float | None = None,
    ) -> BaseLocalFile:
        """Download a remote file and return a local file handle.

        Skips re-download if a matching local copy already exists.

        Parameters
        ----------
        file : BaseRemoteFile
            The remote file to download.
        token : str, optional
            Access token for authenticated clients (e.g. DadosGov).
        callback : Callable, optional
            Progress callback invoked during the download.
        timeout : float, optional
            Maximum seconds to wait for the download. ``None`` (default)
            means no timeout.

        Returns
        -------
        BaseLocalFile
            The downloaded file wrapped in the appropriate handler.

        Raises
        ------
        ValueError
            If the file's client is not recognised.
        RuntimeError
            If the download fails for any reason.
        """

        from pysus.api.extensions import ExtensionFactory

        existing_local = await self.get_local_file(file)
        if existing_local and existing_local.path.exists():
            if existing_local.size == file.size:
                return existing_local
            await self._delete_record(str(existing_local.path))
            existing_local.path.unlink(missing_ok=True)

        client_name = file.client.name.lower()
        remote_path = file.path
        local_path = self._get_dest_path(file)

        local_path.parent.mkdir(parents=True, exist_ok=True)

        await self._update_state(
            local_path,
            str(remote_path),
            client_name,
            DownloadStatus.DOWNLOADING,
        )

        client: DuckLake | FTPClient | DadosGovClient

        try:
            if client_name == "ducklake":
                client = await self.get_ducklake()
            elif client_name == "ftp":
                client = await self.get_ftp()
            elif client_name == "dadosgov":
                client = await self.get_dadosgov(token)
            else:
                raise ValueError(
                    f"No download logic for client: {client_name}",
                )

            if timeout is not None:
                with anyio.fail_after(timeout):
                    await client._download_file(file, local_path, callback)
            else:
                await client._download_file(file, local_path, callback)

            await self._update_state(
                local_path=local_path,
                remote_path=str(remote_path),
                client_name=client_name,
                status=DownloadStatus.DOWNLOADING,
                year=file.year,
                month=file.month,
                state=file.state,
                group=getattr(file.group, "name", None),
            )
            return await ExtensionFactory.instantiate(local_path)

        except Exception as e:  # noqa: B902
            await self._update_state(
                local_path,
                str(remote_path),
                client_name,
                DownloadStatus.FAILED,
            )
            local_path.unlink(missing_ok=True)
            raise RuntimeError(
                f"Unexpected error downloading {file.basename}: {e}",
            ) from e

    async def _delete_record(self, path: str):
        """Delete a LocalFileState record from the database."""

        with self.Session() as session:
            record = session.query(LocalFileState).filter_by(path=path).first()
            if record:
                session.delete(record)
                session.commit()

    async def download_to_parquet(
        self,
        file: BaseRemoteFile,
        token: str | None = None,
        callback: Callable[[int, int], None] | None = None,
        timeout: float | None = None,
        add_dv: bool = True,
    ) -> Parquet:
        """Download a file and convert it to Parquet format.

        Parameters
        ----------
        file : BaseRemoteFile
            The remote file to download and convert.
        token : str, optional
            Access token for authenticated clients.
        callback : Callable[[int, int], None], optional
            Progress callback.
        timeout : float, optional
            Maximum seconds to wait for the download.
        add_dv : bool, optional
            Whether to apply the IBGE verification digit on load
            (default True).

        Returns
        -------
        Parquet
            The converted Parquet file handler.

        Raises
        ------
        NotImplementedError
            If the downloaded file type cannot be converted to Parquet.
        """

        local_file = await self.download(
            file=file,
            token=token,
            callback=callback,
            timeout=timeout,
        )

        if hasattr(local_file, "to_parquet"):
            original_path = local_file.path
            parquet_file = await local_file.to_parquet(callback=callback)
            parquet_file.add_dv = add_dv

            await self._update_state(
                local_path=parquet_file.path,
                remote_path=str(file.path),
                client_name=file.client.name.lower(),
                status=DownloadStatus.COMPLETED,
                year=file.year,
                month=file.month,
                state=file.state,
                group=getattr(file.group, "name", None),
            )

            if original_path.exists() and original_path != parquet_file.path:
                original_path.unlink()
                await self._delete_record(str(original_path))

            return parquet_file

        raise NotImplementedError(
            f"{local_file} can't be converted to Parquet",
        )

    def get_local_hierarchy(self):
        """Build a nested dict of cached files grouped by client and dataset.

        Returns
        -------
        dict
            Nested dict keyed by
            ``{client: {dataset: {group: [files]}}}``.
        """

        with self.Session() as session:
            records = session.query(LocalFileState).all()

        hierarchy = {}
        for r in records:
            client = r.client_name.upper()
            path_obj = Path(str(r.path))
            parts = path_obj.parts

            dataset = parts[-2] if len(parts) > 2 else "Other"
            has_group = getattr(r, "group", None) is not None

            if path_obj.is_file() and len(parts) > 3:
                dataset = parts[-2] if has_group else parts[-3]

            client_dict = hierarchy.setdefault(client, {})
            ds_dict = client_dict.setdefault(dataset, {})
            group_list = ds_dict.setdefault(r.group or "", [])

            group_list.append(
                {
                    "name": path_obj.name,
                    "status": r.status,
                    "path": r.path,
                    "record": r,
                }
            )
        return hierarchy

    def get_completed_remote_paths(self) -> set[str]:
        """Return remote paths for all successfully downloaded files."""

        with self.Session() as session:
            records = (
                session.query(LocalFileState.remote_path)
                .filter(LocalFileState.status == DownloadStatus.COMPLETED)
                .all()
            )
            return {str(r.remote_path) for r in records}

    async def query(
        self,
        client: Origin | None = None,
        dataset: str | None = None,
        group: str | None = None,
        state: str | None = None,
        year: int | None = None,
        month: int | None = None,
    ):
        """Query available datasets through the DuckLake catalog.

        Parameters
        ----------
        client : Origin, optional
            Source client to filter by.
        dataset : str, optional
            Dataset name to filter by.
        group : str, optional
            Group name pattern to filter by (case-insensitive ILIKE).
        state : str, optional
            Two-letter state code to filter by.
        year : int, optional
            Year to filter by.
        month : int, optional
            Month to filter by.

        Returns
        -------
        list
            List of matching File objects.
        """
        if self._ducklake is None:
            await self.get_ducklake()

        if self._ducklake is None:
            raise ConnectionError("Could not connect to PySUS s3 bucket")

        all_datasets = await self._ducklake.datasets()

        if dataset:
            matching = [
                d for d in all_datasets if d.name.lower() == dataset.lower()
            ]
            if not matching:
                return []
            target = matching[0]
            files = await target.query(
                group=group,
                state=state,
                year=year,
                month=month,
            )
        else:
            files = []
            for ds in all_datasets:
                ds_files = await ds.query(
                    group=group,
                    state=state,
                    year=year,
                    month=month,
                )
                files.extend(ds_files)

        if not client:
            return files

        prefix = f"public/data/{client.lower()}/"
        return [f for f in files if f.record.path.startswith(prefix)]

    def read_parquet(
        self,
        paths: list[Path],
        sql: str | None = None,
        mode: Literal["union", "intersection", "strict"] = "union",
        add_dv: bool = True,
    ) -> "DuckDBPyConnection | pd.DataFrame":
        """Read Parquet files with optional schema handling and SQL filter.

        Parameters
        ----------
        paths : list of Path
            One or more Parquet file paths to read.
        sql : str, optional
            Optional SQL filter expression applied to the result.
        mode : {"union", "intersection", "strict"}, optional
            Schema resolution mode (default ``"union"``).
        add_dv : bool, optional
            When True, automatically applies the IBGE verification digit to
            municipality code columns. If matching columns are found, a
            DataFrame is returned instead of a ``DuckDBPyConnection``.

        Returns
        -------
        DuckDBPyConnection or pd.DataFrame
            The query result.

        Raises
        ------
        ValueError
            If no paths are provided, or if the schema mode is ``"strict"``
            and the files have differing schemas.
        """

        from pysus.api.utils import add_dv as _add_dv_fn
        from pysus.api.utils import is_geocode_column

        if not paths:
            raise ValueError("No paths provided")

        def get_columns(path: Path) -> set[tuple[str, str]]:
            """Return the schema of a Parquet file as (name, type) pairs."""
            result = duckdb.execute(f"SELECT * FROM '{path}' LIMIT 0")
            return {(col[0], str(col[1])) for col in result.description}

        if len(paths) == 1:
            query = f"SELECT * FROM '{paths[0]}'"
        else:
            paths_str = ", ".join(f"'{p}'" for p in paths)
            query = f"SELECT * FROM read_parquet([{paths_str}])"

        schemas = [get_columns(p) for p in paths]
        common_columns = set.intersection(*schemas) if schemas else set()

        if mode == "strict":
            for i, schema in enumerate(schemas):
                if schema != schemas[0]:
                    raise ValueError(
                        f"Schema mismatch: file {i} has columns "
                        f"{[c[0] for c in schema]}, "
                        f"expected {[c[0] for c in schemas[0]]}"
                    )

        elif mode == "intersection":
            if not common_columns:
                return duckdb.execute("SELECT * WHERE 1=0")
            cols = ", ".join(f'"{c[0]}"' for c in sorted(common_columns))
            paths_str = ", ".join(f"'{p}'" for p in paths)
            query = f"SELECT {cols} FROM read_parquet([{paths_str}])"

        else:
            paths_str = ", ".join(f"'{p}'" for p in paths)
            query = (
                f"SELECT * FROM read_parquet([{paths_str}], union_by_name=True)"
            )

        if sql:
            if sql.upper().startswith("SELECT"):
                query = sql.replace("FROM t", f"FROM ({query}) AS t")
            else:
                query = f"SELECT {sql} FROM ({query}) AS t"

        base = duckdb.execute(query)

        if not add_dv:
            return base

        geocode_cols = [
            col[0] for col in base.description if is_geocode_column(col[0])
        ]
        if not geocode_cols:
            return base

        try:
            duckdb.create_function(
                "__pysus_add_dv",
                _add_dv_fn,
                null_handling=func.SPECIAL,
            )  # type: ignore
        except duckdb.NotImplementedException:
            pass
        selects = [
            (
                f'__pysus_add_dv("{c[0]}") AS "{c[0]}"'
                if c[0] in geocode_cols
                else f'"{c[0]}"'
            )
            for c in base.description
        ]
        query = f"SELECT {', '.join(selects)} FROM ({query}) AS _t"
        return duckdb.execute(query)
