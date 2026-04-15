import enum
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from pysus import CACHEPATH
from sqlalchemy import DateTime, Enum, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from .dadosgov import DadosGovClient
from .ducklake import DuckLakeClient
from .extensions import Parquet
from .ftp import FTPClient
from .models import BaseLocalFile, BaseRemoteFile, BaseTabularFile


class Base(DeclarativeBase):
    pass


class DownloadStatus(enum.Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"
    MISSING = "missing"


class LocalFileState(Base):
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
        default=datetime.utcnow,
    )


class PySUS:
    def __init__(self, db_path: Path = CACHEPATH / "config.db"):
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self.cachepath = db_path.parent
        self.engine = create_engine(f"duckdb:///{db_path}")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

        self._ducklake: DuckLakeClient | None = None
        self._ftp: FTPClient | None = None
        self._dadosgov: DadosGovClient | None = None

    async def __aenter__(self):
        self._ducklake = DuckLakeClient()
        await self._ducklake._load_catalog()
        self._attach_client_catalog(
            "ducklake", str(self._ducklake.catalog_path)
        )
        return self

    async def get_ducklake(self) -> DuckLakeClient:
        if self._ducklake is None:
            self._ducklake = DuckLakeClient()
            await self._ducklake._load_catalog()
            self._attach_client_catalog(
                "ducklake",
                str(self._ducklake.catalog_path),
            )
        return self._ducklake

    async def get_dadosgov(self, access_token: str | None) -> DadosGovClient:
        if self._dadosgov is None:
            self._dadosgov = DadosGovClient()
            await self._dadosgov.connect(token=access_token)
        return self._dadosgov

    async def get_ftp(self) -> FTPClient:
        if self._ftp is None:
            self._ftp = FTPClient()
            await self._ftp.connect()
        return self._ftp

    async def get_local_file(
        self,
        file: BaseRemoteFile,
    ) -> BaseLocalFile | None:
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
        abs_path = str(Path(path).absolute())
        with self.engine.connect() as conn:
            q = "SELECT database_name FROM duckdb_databases() WHERE path = ?"
            existing = conn.exec_driver_sql(q, (abs_path,)).fetchone()

            if not existing:
                conn.exec_driver_sql(
                    f"ATTACH '{abs_path}' AS {name} (READ_ONLY)",
                )

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._ducklake:
            await self._ducklake.close()
        if self._ftp:
            await self._ftp.close()
        if self._dadosgov:
            await self._dadosgov.close()
        self.engine.dispose()

    def _get_dest_path(self, file: BaseRemoteFile) -> Path:
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
            record.last_synced = datetime.utcnow()
            session.commit()

    async def download(
        self,
        file: BaseRemoteFile,
        token: str | None = None,
        callback: Callable | None = None,
    ):
        from pysus.api.extensions import ExtensionFactory

        existing_local = await self.get_local_file(file)
        if existing_local and existing_local.path.exists():
            return existing_local

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

        client: DuckLakeClient | FTPClient | DadosGovClient

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
                local_path, str(remote_path), client_name, DownloadStatus.FAILED
            )
            raise RuntimeError(
                f"Unexpected error downloading {file.basename}: {e}",
            ) from e

    async def _delete_record(self, path: str):
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
    ) -> Parquet:
        local_file = await self.download(
            file=file,
            token=token,
            callback=callback,
        )

        if not isinstance(local_file, BaseTabularFile):
            raise NotImplementedError(
                f"{local_file} can't be converted to Parquet",
            )

        original_path = local_file.path

        parquet_file = await local_file.to_parquet(callback=callback)

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

    def get_local_hierarchy(self):
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
        with self.Session() as session:
            records = (
                session.query(LocalFileState.remote_path)
                .filter(LocalFileState.status == DownloadStatus.COMPLETED)
                .all()
            )
            return {str(r.remote_path) for r in records}
