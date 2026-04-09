import enum
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from pysus import CACHEPATH
from sqlalchemy import Column, Integer, DateTime, Enum, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from .dadosgov import DadosGovClient
from .ducklake import DuckLakeClient
from .ftp import FTPClient
from .models import BaseLocalFile, BaseRemoteFile

Base = declarative_base()


class DownloadStatus(enum.Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"
    MISSING = "missing"


class LocalFileState(Base):
    __tablename__ = "local_file_state"
    path = Column(String, primary_key=True)
    remote_path = Column(String, nullable=False)
    client_name = Column(String, nullable=False)

    year = Column(Integer, nullable=True)
    month = Column(Integer, nullable=True)
    state = Column(String, nullable=True)
    group = Column(String, nullable=True)

    status = Column(Enum(DownloadStatus), default=DownloadStatus.PENDING)
    sha256 = Column(String, nullable=True)
    last_synced = Column(DateTime, default=datetime.utcnow)


class PySUS:
    def __init__(self, db_path: str = CACHEPATH / "config.db"):
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self.engine = create_engine(f"duckdb:///{db_path}")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

        self._ducklake: DuckLakeClient | None = None
        self._ftp: FTPClient | None = None
        self._dadosgov: DadosGovClient | None = None

    async def __aenter__(self):
        self._ducklake = DuckLakeClient(engine=self.engine)
        await self._ducklake._load_catalog()
        self._attach_client_catalog("ducklake", self._ducklake.catalog_path)
        return self

    async def get_dadosgov(self, access_token: str) -> DadosGovClient:
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
                    remote_path=remote_path,
                    client_name=client_name,
                    status=DownloadStatus.COMPLETED,
                )
                .all()
            )

            if not records:
                return None

            parquet_version = next(
                (r for r in records if r.path.endswith(".parquet")), None
            )
            file = parquet_version or records[0]

            return await ExtensionFactory.instantiate(file.path)

    def _attach_client_catalog(self, name: str, path: str):
        abs_path = str(Path(path).absolute())
        with self.engine.connect() as conn:
            q = "SELECT database_name FROM duckdb_databases() WHERE path = ?"
            existing = conn.exec_driver_sql(q, (abs_path,)).fetchone()

            if not existing:
                conn.exec_driver_sql(f"ATTACH '{abs_path}' AS {
                                     name} (READ_ONLY)")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._ducklake:
            await self._ducklake.close()
        if self._ftp:
            await self._ftp.close()
        if self._dadosgov:
            await self._dadosgov.close()
        self.engine.dispose()

    def _get_dest_path(self, client_name: str, remote_path: str) -> Path:
        return CACHEPATH / "downloads" / client_name / remote_path.lstrip("/")

    async def _update_state(
        self,
        local_path: Path,
        remote_path: str,
        client_name: str,
        status: DownloadStatus,
        year: int = None,
        month: int = None,
        state: str = None,
        group: str = None,
    ):
        with self.Session() as session:
            record = (
                session.query(LocalFileState).filter_by(
                    path=str(local_path)).first()
            )
            if not record:
                record = LocalFileState(
                    path=str(local_path),
                    remote_path=remote_path,
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
        token: str = None,
        callback: Callable = None,
    ):
        from pysus.api.extensions import ExtensionFactory

        existing_local = await self.get_local_file(file)
        if existing_local and existing_local.path.exists():
            return existing_local

        client_name = file.client.name.lower()
        remote_path = file.path
        local_path = self._get_dest_path(client_name, remote_path)

        local_path.parent.mkdir(parents=True, exist_ok=True)

        await self._update_state(
            local_path, remote_path, client_name, DownloadStatus.DOWNLOADING
        )

        try:
            if client_name == "ducklake":
                await self._ducklake._download_file(file, local_path, callback)
            elif client_name == "ftp":
                client = await self.get_ftp()
                await client._download_file(file, local_path, callback)
            elif client_name == "dadosgov":
                client = await self.get_dadosgov(token)
                await client._download_file(file, local_path, callback)
            else:
                raise ValueError(f"No download logic for client: {client_name}")

            await self._update_state(
                local_path=local_path,
                remote_path=remote_path,
                client_name=client_name,
                status=DownloadStatus.DOWNLOADING,
                year=file.year,
                month=file.month,
                state=file.state,
                group=getattr(file.group, "name", None),
            )
            return await ExtensionFactory.instantiate(local_path)

        except Exception:
            await self._update_state(
                local_path, remote_path, client_name, DownloadStatus.FAILED
            )
            raise

    async def download_to_parquet(
        self,
        file: BaseRemoteFile,
        token: str = None,
        callback: Callable = None,
    ):
        local_file = await self.download(
            file=file,
            token=token,
            callback=callback,
        )

        if hasattr(local_file, "to_parquet"):
            parquet_file = await local_file.to_parquet()

            await self._update_state(
                local_path=parquet_file.path,
                remote_path=file.path,
                client_name=file.client.name.lower(),
                status=DownloadStatus.COMPLETED,
            )
            return parquet_file
        return local_file
