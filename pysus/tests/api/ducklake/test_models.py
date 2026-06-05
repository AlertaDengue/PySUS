"""Tests for DuckLake model wrappers (File, DuckDataset, DuckGroup)."""

import hashlib
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, create_autospec, patch

import pytest
from pysus.api.ducklake.catalog.orm.dataset import Dataset
from pysus.api.ducklake.catalog.orm.dataset import File as CatalogFile
from pysus.api.ducklake.catalog.orm.dataset import Group
from pysus.api.ducklake.models import DuckDataset, DuckGroup, File

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog_file_record():
    rec = CatalogFile(
        path="remote/data/file.csv",
        type="csv",
        size=2048,
        rows=500,
        sha256="abc123deadbeef",
        modified=datetime(2024, 6, 1, 12, 0, 0),
        origin_size=2048,
        origin_path="remote/data/file.csv",
    )
    return rec


@pytest.fixture
def record():
    rec = Dataset(
        name="sinan",
        long_name="SINAN",
        description="SINAN dataset",
    )
    return rec


@pytest.fixture
def group_record():
    rec = Group(
        name="acidentes",
        long_name="Acidentes",
        description="Acidentes de trânsito",
    )
    return rec


@pytest.fixture
def mock_client():
    from pysus.api.ducklake.client import DuckLake

    mc = create_autospec(DuckLake, instance=True)
    mc._datasets = []
    return mc


@pytest.fixture
def mock_dataset(mock_client, record):
    with patch("pathlib.Path.mkdir"):
        ds = DuckDataset(record=record, client=mock_client)
    return ds


@pytest.fixture
def mock_group(group_record, mock_dataset):
    with patch("pathlib.Path.mkdir"):
        g = DuckGroup(record=group_record, dataset=mock_dataset)
    return g


# ---------------------------------------------------------------------------
# File
# ---------------------------------------------------------------------------


class TestFile:
    def test_init(self, catalog_file_record, mock_dataset):
        f = File(dataset=mock_dataset, record=catalog_file_record)
        assert f.record is catalog_file_record
        assert f.dataset is mock_dataset
        assert f.group is None

    def test_init_with_group(
        self, catalog_file_record, mock_dataset, mock_group
    ):
        f = File(
            dataset=mock_dataset,
            record=catalog_file_record,
            group=mock_group,
        )
        assert f.group is mock_group

    def test_path(self, catalog_file_record, mock_dataset):
        f = File(dataset=mock_dataset, record=catalog_file_record)
        assert f.path == Path("remote/data/file.csv")

    def test_basename(self, catalog_file_record, mock_dataset):
        f = File(dataset=mock_dataset, record=catalog_file_record)
        assert f.basename == "file.csv"

    def test_extension(self, catalog_file_record, mock_dataset):
        f = File(dataset=mock_dataset, record=catalog_file_record)
        assert f.extension == ".csv"

    def test_size(self, catalog_file_record, mock_dataset):
        f = File(dataset=mock_dataset, record=catalog_file_record)
        assert f.size == 2048

    def test_modify(self, catalog_file_record, mock_dataset):
        f = File(dataset=mock_dataset, record=catalog_file_record)
        assert f.modify == datetime(2024, 6, 1, 12, 0, 0)

    def test_rows(self, catalog_file_record, mock_dataset):
        f = File(dataset=mock_dataset, record=catalog_file_record)
        assert f.rows == 500

    def test_sha256(self, catalog_file_record, mock_dataset):
        f = File(dataset=mock_dataset, record=catalog_file_record)
        assert f.sha256 == "abc123deadbeef"

    def test_sha256_none(self, catalog_file_record, mock_dataset):
        catalog_file_record.sha256 = None
        f = File(dataset=mock_dataset, record=catalog_file_record)
        assert f.sha256 is None

    def test_name_fallback(self, catalog_file_record, mock_dataset):
        f = File(dataset=mock_dataset, record=catalog_file_record)
        assert f.name == "file.csv"

    @pytest.mark.asyncio
    async def test_download_with_explicit_output(
        self, catalog_file_record, mock_dataset
    ):
        f = File(dataset=mock_dataset, record=catalog_file_record)
        output = Path("/tmp/out.csv")
        cb = MagicMock()
        mock_dataset.client._download_file.return_value = output
        result = await f._download(output=output, callback=cb)
        mock_dataset.client._download_file.assert_awaited_once_with(
            f, output, callback=cb
        )
        assert result == output

    @pytest.mark.asyncio
    async def test_download_without_output(
        self, catalog_file_record, mock_dataset
    ):
        from pysus import CACHEPATH

        f = File(dataset=mock_dataset, record=catalog_file_record)
        expected = CACHEPATH / f.name
        mock_dataset.client._download_file.return_value = expected
        result = await f._download()
        mock_dataset.client._download_file.assert_awaited_once_with(
            f, expected, callback=None
        )
        assert result == expected

    @pytest.mark.asyncio
    async def test_verify_no_hash_returns_true(
        self, catalog_file_record, mock_dataset, tmp_path
    ):
        catalog_file_record.sha256 = None
        f = File(dataset=mock_dataset, record=catalog_file_record)
        result = await f.verify(tmp_path / "whatever")
        assert result is True

    @pytest.mark.asyncio
    async def test_verify_matching_hash(self, mock_dataset, tmp_path):
        content = b"hello world, this is test content"
        expected_hash = hashlib.sha256(content).hexdigest()

        record = CatalogFile(
            path="remote/data/file.csv",
            type="csv",
            sha256=expected_hash,
            size=len(content),
            rows=1,
            modified=datetime.now(),
            origin_size=len(content),
            origin_path="remote/data/file.csv",
        )

        file_path = tmp_path / "test_file.csv"
        file_path.write_bytes(content)

        f = File(dataset=mock_dataset, record=record)
        assert await f.verify(file_path) is True

    @pytest.mark.asyncio
    async def test_verify_mismatching_hash(self, mock_dataset, tmp_path):
        content = b"hello world, this is test content"
        wrong_content = b"this content does not match"
        expected_hash = hashlib.sha256(content).hexdigest()

        record = CatalogFile(
            path="remote/data/file.csv",
            type="csv",
            sha256=expected_hash,
            size=len(wrong_content),
            rows=1,
            modified=datetime.now(),
            origin_size=len(wrong_content),
            origin_path="remote/data/file.csv",
        )

        file_path = tmp_path / "test_file.csv"
        file_path.write_bytes(wrong_content)

        f = File(dataset=mock_dataset, record=record)
        assert await f.verify(file_path) is False


# ---------------------------------------------------------------------------
# DuckDataset
# ---------------------------------------------------------------------------


class TestDuckDataset:
    def test_init(self, mock_client, record):
        with patch("pathlib.Path.mkdir"):
            ds = DuckDataset(record=record, client=mock_client)
        assert ds.record is record
        assert ds.client is mock_client
        assert ds._catalog_name == "catalog_sinan.duckdb"

    def test_repr(self, mock_dataset):
        assert repr(mock_dataset) == "SINAN"

    def test_name(self, mock_dataset):
        assert mock_dataset.name == "sinan"

    def test_long_name(self, mock_dataset):
        assert mock_dataset.long_name == ""

    def test_description(self, mock_dataset):
        assert mock_dataset.description == ""

    def test_catalog_path(self, mock_dataset):
        from pysus import CACHEPATH

        expected = Path(CACHEPATH) / "ducklake" / "catalog_sinan.duckdb"
        assert mock_dataset.catalog_path == expected

    @pytest.mark.asyncio
    async def test_connect_already_connected(self, mock_dataset, mock_client):
        mock_dataset._engine = MagicMock()
        mock_dataset._Session = MagicMock()
        await mock_dataset.connect(force=False)
        mock_client._download.assert_not_called()

    @pytest.mark.asyncio
    async def test_connect_force_reconnects(self, mock_dataset, mock_client):
        mock_dataset._engine = MagicMock()
        mock_dataset._Session = MagicMock()

        def run_sync(fn, *args, **kwargs):
            return fn()

        with patch(
            "pysus.api.ducklake.models.to_thread.run_sync",
            side_effect=run_sync,
        ):
            with patch.object(
                mock_client, "_setup_engine", return_value=MagicMock()
            ):
                await mock_dataset.connect(force=True)

        mock_client._download.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_creates_session_if_missing(
        self, mock_dataset, mock_client
    ):
        mock_dataset._engine = MagicMock()
        mock_dataset._Session = None
        await mock_dataset.connect(force=False)
        assert mock_dataset._Session is not None
        mock_client._download.assert_not_called()

    @pytest.mark.asyncio
    async def test_connect_full_path(self, mock_dataset, mock_client):
        mock_dataset._engine = None

        def run_sync(fn, *args, **kwargs):
            return fn()

        with patch(
            "pysus.api.ducklake.models.to_thread.run_sync",
            side_effect=run_sync,
        ):
            with patch.object(
                mock_client, "_setup_engine", return_value=MagicMock()
            ):
                await mock_dataset.connect()

        mock_client._download.assert_awaited_once()
        assert mock_dataset._engine is not None
        assert mock_dataset._Session is not None
        assert mock_dataset in mock_client._datasets

    @pytest.mark.asyncio
    async def test_close_disposes_engine(self, mock_dataset):
        engine = MagicMock()
        mock_dataset._engine = engine
        with patch(
            "pysus.api.ducklake.models.to_thread.run_sync",
            side_effect=lambda fn, *a, **kw: fn(),
        ):
            await mock_dataset.close()
        engine.dispose.assert_called_once()
        assert mock_dataset._engine is None
        assert mock_dataset._Session is None

    @pytest.mark.asyncio
    async def test_close_noop_when_no_engine(self, mock_dataset):
        mock_dataset._engine = None
        await mock_dataset.close()

    @pytest.mark.asyncio
    async def test_close_with_update_catalog(self, mock_dataset, mock_client):
        engine = MagicMock()
        mock_dataset._engine = engine
        mock_client._is_authenticated = True

        with patch.object(mock_dataset, "_upload_catalog") as mock_upload:
            with patch(
                "pysus.api.ducklake.models.to_thread.run_sync",
                side_effect=lambda fn, *a, **kw: fn(),
            ):
                await mock_dataset.close(update_catalog=True)
            engine.dispose.assert_called_once()
            mock_upload.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_upload_catalog_no_credentials_raises(self, mock_dataset):
        mock_dataset.client.credentials = None
        with pytest.raises(PermissionError, match="Admin credentials required"):
            await mock_dataset._upload_catalog()

    @pytest.mark.asyncio
    async def test_upload_catalog_success(
        self, mock_dataset, mock_client, tmp_path
    ):
        mock_client.credentials = MagicMock()
        mock_client._s3_client = MagicMock()
        mock_client.bucket = "pysus"
        local_db = tmp_path / "catalog_sinan.duckdb"
        local_db.write_text("data")
        mock_dataset._catalog_local = local_db

        def run_sync(fn, *args, **kwargs):
            return fn()

        with patch(
            "pysus.api.ducklake.models.to_thread.run_sync",
            side_effect=run_sync,
        ):
            await mock_dataset._upload_catalog()

        mock_client._s3_client.upload_file.assert_called_once_with(
            str(local_db),
            mock_client.bucket,
            f"catalog_{mock_dataset.record.name.lower()}.duckdb",
        )

    @pytest.mark.asyncio
    async def test_query_no_filters(self, mock_dataset):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.all.return_value = []

        mock_dataset._Session = MagicMock(return_value=mock_session)

        def run_sync(fn, *args, **kwargs):
            return fn()

        with patch(
            "pysus.api.ducklake.models.to_thread.run_sync",
            side_effect=run_sync,
        ):
            result = await mock_dataset.query()

        assert result == []

    @pytest.mark.asyncio
    async def test_query_with_all_filters(self, mock_dataset):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.all.return_value = []

        mock_dataset._Session = MagicMock(return_value=mock_session)

        def run_sync(fn, *args, **kwargs):
            return fn()

        with patch(
            "pysus.api.ducklake.models.to_thread.run_sync",
            side_effect=run_sync,
        ):
            result = await mock_dataset.query(
                group="acidentes%",
                state="RJ",
                year=2024,
                month=6,
            )

        assert result == []

    @pytest.mark.asyncio
    async def test_query_connects_if_no_session(self, mock_dataset):
        mock_dataset._Session = None

        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.all.return_value = []

        async def _connect(*args, **kwargs):
            mock_dataset._Session = MagicMock(return_value=mock_session)

        with patch.object(
            DuckDataset, "connect", new=AsyncMock(side_effect=_connect)
        ) as mock_connect:
            with patch(
                "pysus.api.ducklake.models.to_thread.run_sync",
                side_effect=lambda fn, *a, **kw: fn(),
            ):
                await mock_dataset.query()
                mock_connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_fetch_content_with_groups_and_files(
        self, mock_dataset, mock_client
    ):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.filter.return_value = mock_query

        group_rec = Group(
            name="dengue",
            long_name="Dengue",
            description="Dengue data",
        )

        file_rec = CatalogFile(
            path="remote/dengue/data.csv",
            type="csv",
            sha256="hash123",
            size=100,
            rows=10,
            modified=datetime.now(),
            origin_size=100,
            origin_path="remote/dengue/data.csv",
        )

        dataset_rec = Dataset(
            name="sinan",
            long_name="SINAN",
            description="SINAN dataset",
        )
        dataset_rec.groups = [group_rec]
        dataset_rec.files = [file_rec]

        mock_query.first.return_value = dataset_rec

        mock_dataset._Session = MagicMock(return_value=mock_session)

        def run_sync(fn, *args, **kwargs):
            return fn()

        with patch(
            "pysus.api.ducklake.models.to_thread.run_sync",
            side_effect=run_sync,
        ):
            items = await mock_dataset._fetch_content()

        assert len(items) == 2
        assert isinstance(items[0], DuckGroup)
        assert items[0].record is group_rec
        assert isinstance(items[1], File)
        assert items[1].record is file_rec

    @pytest.mark.asyncio
    async def test_fetch_content_no_dataset(self, mock_dataset):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = None

        mock_dataset._Session = MagicMock(return_value=mock_session)

        def run_sync(fn, *args, **kwargs):
            return fn()

        with patch(
            "pysus.api.ducklake.models.to_thread.run_sync",
            side_effect=run_sync,
        ):
            items = await mock_dataset._fetch_content()

        assert items == []

    @pytest.mark.asyncio
    async def test_fetch_content_only_groups(self, mock_dataset):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.filter.return_value = mock_query

        group_rec = Group(
            name="dengue",
            long_name="Dengue",
            description="Dengue data",
        )

        dataset_rec = Dataset(
            name="sinan",
            long_name="SINAN",
            description="Test",
        )
        dataset_rec.groups = [group_rec]
        dataset_rec.files = []

        mock_query.first.return_value = dataset_rec

        mock_dataset._Session = MagicMock(return_value=mock_session)

        def run_sync(fn, *args, **kwargs):
            return fn()

        with patch(
            "pysus.api.ducklake.models.to_thread.run_sync",
            side_effect=run_sync,
        ):
            items = await mock_dataset._fetch_content()

        assert len(items) == 1
        assert isinstance(items[0], DuckGroup)

    @pytest.mark.asyncio
    async def test_fetch_content_connects_if_no_session(self, mock_dataset):
        mock_dataset._Session = None

        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.options.return_value = mock_query
        mock_query.filter.return_value = mock_query
        ds = Dataset(
            name="sinan",
            long_name="SINAN",
            description="Test",
        )
        ds.groups = []
        ds.files = []
        mock_query.first.return_value = ds

        async def _connect(*args, **kwargs):
            mock_dataset._Session = MagicMock(return_value=mock_session)

        with patch.object(
            DuckDataset, "connect", new=AsyncMock(side_effect=_connect)
        ) as mock_connect:
            with patch(
                "pysus.api.ducklake.models.to_thread.run_sync",
                side_effect=lambda fn, *a, **kw: fn(),
            ):
                await mock_dataset._fetch_content()
                mock_connect.assert_awaited_once()


# ---------------------------------------------------------------------------
# DuckGroup
# ---------------------------------------------------------------------------


class TestDuckGroup:
    def test_name(self, mock_group):
        assert mock_group.name == "acidentes"

    def test_long_name(self, mock_group):
        assert mock_group.long_name == "Acidentes"

    def test_long_name_fallback(self, mock_group):
        mock_group.record.long_name = None
        assert mock_group.long_name == "acidentes"

    def test_description(self, mock_group):
        assert mock_group.description == "Acidentes de trânsito"

    @pytest.mark.asyncio
    async def test_fetch_files(self, mock_group, mock_dataset):
        file_rec = CatalogFile(
            path="remote/data/file.csv",
            type="csv",
            size=100,
            rows=10,
            modified=datetime.now(),
            origin_size=100,
            origin_path="remote/data/file.csv",
        )
        mock_group.record.files = [file_rec]

        files = await mock_group._fetch_files()
        assert len(files) == 1
        assert isinstance(files[0], File)
        assert files[0].record is file_rec
        assert files[0].group is mock_group
        assert files[0].dataset is mock_dataset
