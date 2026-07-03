import pathlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pysus.api.client import DownloadStatus, LocalFileState, PySUS
from pysus.api.errors import (
    ConnectionError,
    DownloadError,
    FormatError,
    ValidationError,
)


@pytest.fixture
def test_db_path(tmp_path):
    return tmp_path / "test_config.db"


class TestPySUS:
    @pytest.mark.asyncio
    async def test_pysus_init(self, test_db_path):
        client = PySUS(db_path=test_db_path)
        assert client.cachepath == test_db_path.parent
        assert client._ducklake is None
        assert client._ftp is None
        assert client._dadosgov is None
        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_get_dest_path_basic(self, test_db_path):
        client = PySUS(db_path=test_db_path)

        mock_file = MagicMock()
        mock_file.client.name = "FTP"
        mock_file.dataset.name = "SINASC"
        mock_file.basename = "DNAC2024.dbc"
        mock_file.group = None

        result = client._get_dest_path(mock_file)
        expected = (
            test_db_path.parent
            / "downloads"
            / "ftp"
            / "sinasc"
            / "DNAC2024.dbc"
        )
        assert result == expected
        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_get_dest_path_with_group(self, test_db_path):
        client = PySUS(db_path=test_db_path)

        mock_file = MagicMock()
        mock_file.client.name = "FTP"
        mock_file.dataset.name = "SINASC"
        mock_file.basename = "DNAC2024.dbc"
        mock_group = MagicMock()
        mock_group.name = "DC"
        mock_file.group = mock_group

        result = client._get_dest_path(mock_file)
        expected = (
            test_db_path.parent
            / "downloads"
            / "ftp"
            / "sinasc"
            / "DC"
            / "DNAC2024.dbc"
        )
        assert result == expected
        await client.__aexit__(None, None, None)


class TestDownloadStatus:
    def test_download_status_values(self):
        assert DownloadStatus.PENDING.value == "pending"
        assert DownloadStatus.DOWNLOADING.value == "downloading"
        assert DownloadStatus.COMPLETED.value == "completed"
        assert DownloadStatus.FAILED.value == "failed"
        assert DownloadStatus.MISSING.value == "missing"


class TestGetLocalFile:
    @pytest.mark.asyncio
    async def test_get_local_file_returns_none_when_no_records(
        self, test_db_path
    ):
        client = PySUS(db_path=test_db_path)

        mock_remote_file = MagicMock()
        mock_remote_file.client.name = "FTP"
        mock_remote_file.path = "/remote/nonexistent.dbc"

        result = await client.get_local_file(mock_remote_file)
        assert result is None

        await client.__aexit__(None, None, None)


class TestLocalFileState:
    @pytest.mark.asyncio
    async def test_update_state_creates_record(self, test_db_path, tmp_path):
        client = PySUS(db_path=test_db_path)

        local = pathlib.Path(tmp_path / "test.dbc")

        await client._update_state(
            local_path=local,
            remote_path="/remote/test.dbc",
            client_name="ftp",
            status=DownloadStatus.COMPLETED,
            year=2024,
            month=1,
            state="SP",
            group="DC",
        )

        with client.Session() as session:
            record = (
                session.query(LocalFileState).filter_by(path=str(local)).first()
            )
            assert record is not None
            assert record.remote_path == "/remote/test.dbc"
            assert record.client_name == "ftp"
            assert record.status == DownloadStatus.COMPLETED
            assert record.year == 2024
            assert record.month == 1
            assert record.state == "SP"
            assert record.group == "DC"

        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_delete_record_removes_entry(self, test_db_path, tmp_path):
        client = PySUS(db_path=test_db_path)

        local = pathlib.Path(tmp_path / "test.dbc")

        await client._update_state(
            local_path=local,
            remote_path="/remote/test.dbc",
            client_name="ftp",
            status=DownloadStatus.COMPLETED,
        )

        await client._delete_record(str(local))

        with client.Session() as session:
            record = (
                session.query(LocalFileState).filter_by(path=str(local)).first()
            )
            assert record is None

        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_get_local_file_finds_existing(self, test_db_path, tmp_path):
        client = PySUS(db_path=test_db_path)

        local = pathlib.Path(tmp_path / "test.dbc")

        await client._update_state(
            local_path=local,
            remote_path="/remote/test.dbc",
            client_name="ftp",
            status=DownloadStatus.COMPLETED,
        )

        mock_remote_file = MagicMock()
        mock_remote_file.client.name = "FTP"
        mock_remote_file.path = "/remote/test.dbc"

        with patch(
            "pysus.api.extensions.ExtensionFactory.instantiate",
            new_callable=AsyncMock,
        ) as mock_factory:
            mock_factory.return_value = MagicMock()
            await client.get_local_file(mock_remote_file)
            mock_factory.assert_called_once()

        await client.__aexit__(None, None, None)


class TestGetCompletedRemotePaths:
    @pytest.mark.asyncio
    async def test_get_completed_remote_paths(self, test_db_path, tmp_path):
        client = PySUS(db_path=test_db_path)

        local1 = pathlib.Path(tmp_path / "test1.dbc")
        local2 = pathlib.Path(tmp_path / "test2.dbc")

        await client._update_state(
            local_path=local1,
            remote_path="/remote/test1.dbc",
            client_name="ftp",
            status=DownloadStatus.COMPLETED,
        )
        await client._update_state(
            local_path=local2,
            remote_path="/remote/test2.dbc",
            client_name="ftp",
            status=DownloadStatus.PENDING,
        )

        paths = client.get_completed_remote_paths()
        assert "/remote/test1.dbc" in paths
        assert "/remote/test2.dbc" not in paths

        await client.__aexit__(None, None, None)


class TestGetLocalHierarchy:
    @pytest.mark.asyncio
    async def test_get_local_hierarchy_all_branches(
        self, test_db_path, tmp_path
    ):
        client = PySUS(db_path=test_db_path)

        file1 = (
            tmp_path / "downloads" / "ftp" / "sinasc" / "DC" / "DNAC2024.dbc"
        )
        file1.parent.mkdir(parents=True, exist_ok=True)
        file1.write_text("dummy")

        file2 = tmp_path / "downloads" / "ftp" / "sinasc" / "DNAC2024.dbc"
        file2.parent.mkdir(parents=True, exist_ok=True)
        file2.write_text("dummy")

        file3 = tmp_path / "short" / "path.dbc"
        file3.parent.mkdir(parents=True, exist_ok=True)
        file3.write_text("dummy")

        dir_path = tmp_path / "downloads" / "ftp" / "sinasc" / "DC"
        dir_path.mkdir(parents=True, exist_ok=True)

        with client.Session() as session:
            r1 = LocalFileState(
                path=str(file1),
                remote_path="/remote/file1.dbc",
                client_name="ftp",
                status=DownloadStatus.COMPLETED,
                group="DC",
            )
            session.add(r1)

            r2 = LocalFileState(
                path=str(file2),
                remote_path="/remote/file2.dbc",
                client_name="ftp",
                status=DownloadStatus.COMPLETED,
                group=None,
            )
            session.add(r2)

            r3 = LocalFileState(
                path=str(file3),
                remote_path="/remote/file3.dbc",
                client_name="ftp",
                status=DownloadStatus.PENDING,
                group="X",
            )
            session.add(r3)

            r4 = LocalFileState(
                path=str(dir_path),
                remote_path="/remote/dir.dbc",
                client_name="ftp",
                status=DownloadStatus.COMPLETED,
                group="DC",
            )
            session.add(r4)

            session.commit()

        hierarchy = client.get_local_hierarchy()

        assert "FTP" in hierarchy
        ftp_dict = hierarchy["FTP"]

        assert "DC" in ftp_dict
        ds_dc = ftp_dict["DC"]
        assert "DC" in ds_dc
        assert len(ds_dc["DC"]) == 1
        assert ds_dc["DC"][0]["name"] == "DNAC2024.dbc"
        assert ds_dc["DC"][0]["status"] == DownloadStatus.COMPLETED

        assert "ftp" in ftp_dict
        ds_ftp = ftp_dict["ftp"]
        assert "" in ds_ftp
        assert len(ds_ftp[""]) == 1
        assert ds_ftp[""][0]["name"] == "DNAC2024.dbc"

        assert "sinasc" in ftp_dict
        ds_sinasc = ftp_dict["sinasc"]
        assert "DC" in ds_sinasc
        assert ds_sinasc["DC"][0]["name"] == "DC"

        dc_dict = ftp_dict.get("short")
        assert dc_dict is not None
        assert "X" in dc_dict
        assert dc_dict["X"][0]["status"] == DownloadStatus.PENDING

        await client.__aexit__(None, None, None)


class TestPySUSQuery:
    @pytest.fixture
    def mock_dataset(self):
        ds = MagicMock()
        ds.name = "sinan"
        ds.query = AsyncMock(return_value=[])
        return ds

    @pytest.mark.asyncio
    async def test_query_with_dataset(
        self, test_db_path, tmp_path, mock_dataset
    ):
        from unittest.mock import MagicMock

        from pysus.api.ducklake.client import DuckLake

        client = PySUS(db_path=test_db_path)

        mock_ducklake = MagicMock(spec=DuckLake)
        mock_file = MagicMock()
        mock_file.path = tmp_path / "test.parquet"
        mock_dataset.query = AsyncMock(return_value=[mock_file])
        mock_ducklake.datasets = AsyncMock(return_value=[mock_dataset])

        client._ducklake = mock_ducklake
        client._attach_client_catalog = MagicMock()

        result = await client.query(dataset="sinan")

        mock_ducklake.datasets.assert_called_once()
        mock_dataset.query.assert_called_once_with(
            group=None,
            state=None,
            year=None,
            month=None,
        )
        assert result == [mock_file]
        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_query_with_group(self, test_db_path, mock_dataset):
        from unittest.mock import MagicMock

        from pysus.api.ducklake.client import DuckLake

        client = PySUS(db_path=test_db_path)

        mock_ducklake = MagicMock(spec=DuckLake)
        mock_ducklake.datasets = AsyncMock(return_value=[mock_dataset])

        client._ducklake = mock_ducklake
        client._attach_client_catalog = MagicMock()

        await client.query(dataset="sinan", group="DENGUE")

        mock_dataset.query.assert_called_once_with(
            group="DENGUE",
            state=None,
            year=None,
            month=None,
        )
        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_query_with_all_params(self, test_db_path):
        from unittest.mock import AsyncMock, MagicMock

        from pysus.api.ducklake.client import DuckLake

        client = PySUS(db_path=test_db_path)

        mock_ducklake = MagicMock(spec=DuckLake)
        ds = MagicMock()
        ds.name = "sinasc"
        ds.query = AsyncMock(return_value=[])
        mock_ducklake.datasets = AsyncMock(return_value=[ds])

        client._ducklake = mock_ducklake
        client._attach_client_catalog = MagicMock()

        await client.query(
            dataset="sinasc",
            group="DC",
            state="SP",
            year=2024,
            month=1,
        )

        ds.query.assert_called_once_with(
            group="DC",
            state="SP",
            year=2024,
            month=1,
        )
        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_query_initializes_ducklake(self, test_db_path, mock_dataset):
        from unittest.mock import AsyncMock, MagicMock, patch

        import duckdb
        from pysus.api.ducklake.client import DuckLake

        client = PySUS(db_path=test_db_path)
        assert client._ducklake is None

        mock_ducklake_instance = MagicMock(spec=DuckLake)
        mock_ducklake_instance.datasets = AsyncMock(return_value=[mock_dataset])
        tmp_catalog_path = test_db_path.parent / "catalog.duckdb"
        mock_ducklake_instance.catalog_path = tmp_catalog_path

        # Create the catalog database
        conn = duckdb.connect(str(tmp_catalog_path))
        conn.close()

        with patch.object(
            DuckLake, "__new__", return_value=mock_ducklake_instance
        ):
            await client.query(dataset="sinan")

        assert client._ducklake is not None
        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_query_raises_connection_error_when_ducklake_stays_none(
        self, test_db_path
    ):
        client = PySUS(db_path=test_db_path)
        client._ducklake = None

        with patch.object(
            client, "get_ducklake", new=AsyncMock(return_value=None)
        ):
            with pytest.raises(
                ConnectionError, match="Could not connect to PySUS s3 bucket"
            ):
                await client.query(dataset="sinan")

        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_query_dataset_not_found_returns_empty(self, test_db_path):
        from unittest.mock import AsyncMock, MagicMock

        from pysus.api.ducklake.client import DuckLake

        client = PySUS(db_path=test_db_path)

        mock_ducklake = MagicMock(spec=DuckLake)
        ds = MagicMock()
        ds.name = "sinasc"
        mock_ducklake.datasets = AsyncMock(return_value=[ds])

        client._ducklake = mock_ducklake
        client._attach_client_catalog = MagicMock()

        result = await client.query(dataset="sinan")
        assert result == []

        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_query_no_dataset_iterates_all(self, test_db_path):
        from unittest.mock import AsyncMock, MagicMock

        from pysus.api.ducklake.client import DuckLake

        client = PySUS(db_path=test_db_path)

        mock_ducklake = MagicMock(spec=DuckLake)
        ds1 = MagicMock()
        ds1.name = "sinan"
        ds1.query = AsyncMock(return_value=["file1"])
        ds2 = MagicMock()
        ds2.name = "sinasc"
        ds2.query = AsyncMock(return_value=["file2", "file3"])
        mock_ducklake.datasets = AsyncMock(return_value=[ds1, ds2])

        client._ducklake = mock_ducklake
        client._attach_client_catalog = MagicMock()

        result = await client.query()

        ds1.query.assert_awaited_once_with(
            group=None,
            state=None,
            year=None,
            month=None,
        )
        ds2.query.assert_awaited_once_with(
            group=None,
            state=None,
            year=None,
            month=None,
        )
        assert result == ["file1", "file2", "file3"]

        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_query_with_client_filter(self, test_db_path):
        from unittest.mock import AsyncMock, MagicMock

        from pysus.api.ducklake.client import DuckLake
        from pysus.api.types import FTP

        client = PySUS(db_path=test_db_path)

        mock_ducklake = MagicMock(spec=DuckLake)
        ds = MagicMock()
        ds.name = "sinan"

        mock_file1 = MagicMock()
        mock_file1.path = "public/data/ftp/somefile"
        mock_file2 = MagicMock()
        mock_file2.path = "public/data/dadosgov/otherfile"

        ds.query = AsyncMock(return_value=[mock_file1, mock_file2])
        mock_ducklake.datasets = AsyncMock(return_value=[ds])

        client._ducklake = mock_ducklake
        client._attach_client_catalog = MagicMock()

        result = await client.query(dataset="sinan", client=FTP)

        assert result == [mock_file1]

        await client.__aexit__(None, None, None)


class TestDownload:
    @pytest.mark.asyncio
    async def test_download_returns_existing_when_size_matches(
        self, test_db_path
    ):
        from unittest.mock import AsyncMock, MagicMock, patch

        client = PySUS(db_path=test_db_path)
        mock_local = MagicMock()
        mock_local.path.exists.return_value = True
        mock_local.size = 1000
        mock_file = MagicMock()
        mock_file.size = 1000
        mock_file.client.name = "ftp"

        with patch.object(
            client, "get_local_file", new=AsyncMock(return_value=mock_local)
        ):
            result = await client.download(mock_file)

        assert result == mock_local

    @pytest.mark.asyncio
    async def test_download_sets_status_to_completed_on_success(
        self, test_db_path
    ):
        from unittest.mock import AsyncMock, MagicMock, patch

        from pysus.api.extensions import ExtensionFactory

        mock_local = MagicMock()
        mock_local.path.exists.return_value = False
        mock_file = MagicMock()
        mock_file.size = 1000
        mock_file.client.name = "ftp"
        mock_file.path = test_db_path.parent / "remote.dbc"
        mock_file.basename = "remote.dbc"

        client = PySUS(db_path=test_db_path)

        mock_update = AsyncMock()

        with (
            patch.object(
                client, "get_local_file", new=AsyncMock(return_value=mock_local)
            ),
            patch.object(
                client,
                "_get_dest_path",
                return_value=test_db_path.parent / "test.dbc",
            ),
            patch.object(client, "_update_state", new=mock_update),
            patch.object(
                ExtensionFactory,
                "instantiate",
                new_callable=AsyncMock,
                return_value=mock_local,
            ),
        ):
            mock_client = AsyncMock()
            client._ftp = mock_client

            await client.download(mock_file)

        assert mock_update.call_count == 2
        final_call = mock_update.call_args_list[1]
        assert final_call.kwargs["status"] == DownloadStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_download_re_fetches_when_size_differs(self, test_db_path):
        import pathlib
        from unittest.mock import AsyncMock, MagicMock, patch

        from pysus.api.extensions import ExtensionFactory

        mock_local = MagicMock()
        mock_local.path.exists.return_value = True
        mock_local.size = 500
        mock_file = MagicMock()
        mock_file.size = 1000
        mock_file.client.name = "ftp"
        mock_file.path = pathlib.Path("/remote/test.dbc")
        mock_file.basename = "test.dbc"

        client = PySUS(db_path=test_db_path)
        get_local_file_patch = patch.object(
            client, "get_local_file", new=AsyncMock(return_value=mock_local)
        )
        delete_record_patch = patch.object(
            client, "_delete_record", new=AsyncMock()
        )
        get_dest_patch = patch.object(
            client,
            "_get_dest_path",
            return_value=test_db_path.parent / "test.dbc",
        )
        update_state_patch = patch.object(
            client, "_update_state", new=AsyncMock()
        )
        get_ftp_patch = patch.object(client, "get_ftp", new=AsyncMock())

        with (
            get_local_file_patch,
            delete_record_patch as mock_delete,
            get_dest_patch,
            update_state_patch,
            get_ftp_patch,
        ):
            with patch.object(
                ExtensionFactory,
                "instantiate",
                new_callable=AsyncMock,
                return_value=mock_local,
            ):
                mock_client = AsyncMock()
                mock_client.download = AsyncMock()
                client._ftp = mock_client
                await client.download(mock_file)

        mock_delete.assert_awaited_once()
        assert mock_local.path.unlink.called

    @pytest.mark.asyncio
    async def test_download_passes_timeout(self, test_db_path):
        from unittest.mock import AsyncMock, MagicMock, patch

        import anyio
        from pysus.api.extensions import ExtensionFactory

        mock_local = MagicMock()
        mock_local.path.exists.return_value = False
        mock_file = MagicMock()
        mock_file.size = 1000
        mock_file.client.name = "ftp"
        mock_file.path = test_db_path.parent / "remote.dbc"
        mock_file.basename = "remote.dbc"

        client = PySUS(db_path=test_db_path)

        async def _slow_download(*args, **kwargs):
            await anyio.sleep(10)

        with (
            patch.object(
                client, "get_local_file", new=AsyncMock(return_value=mock_local)
            ),
            patch.object(
                client,
                "_get_dest_path",
                return_value=test_db_path.parent / "test.dbc",
            ),
            patch.object(client, "_update_state", new=AsyncMock()),
            patch.object(
                ExtensionFactory,
                "instantiate",
                new_callable=AsyncMock,
                return_value=mock_local,
            ),
        ):
            mock_client = AsyncMock()
            mock_client.download = _slow_download
            client._ftp = mock_client

            with pytest.raises(
                DownloadError, match="Unexpected error downloading"
            ):
                await client.download(mock_file, timeout=0.001)

    @pytest.mark.asyncio
    async def test_download_with_ducklake_client(self, test_db_path):
        from unittest.mock import AsyncMock, MagicMock, patch

        from pysus.api.extensions import ExtensionFactory

        client = PySUS(db_path=test_db_path)

        mock_local = MagicMock()
        mock_local.path.exists.return_value = False

        mock_file = MagicMock()
        mock_file.client.name = "ducklake"
        mock_file.size = 1000
        mock_file.path = test_db_path.parent / "remote.ducklake"
        mock_file.basename = "remote.ducklake"
        mock_file.year = None
        mock_file.month = None
        mock_file.state = None
        mock_group = MagicMock()
        mock_group.name = None
        mock_file.group = MagicMock()

        with (
            patch.object(
                client, "get_local_file", new=AsyncMock(return_value=mock_local)
            ),
            patch.object(
                client,
                "_get_dest_path",
                return_value=test_db_path.parent / "test.ducklake",
            ),
            patch.object(client, "_update_state", new=AsyncMock()),
            patch.object(
                ExtensionFactory,
                "instantiate",
                new_callable=AsyncMock,
                return_value=mock_local,
            ),
        ):
            mock_ducklake = AsyncMock()
            mock_ducklake.download = AsyncMock()
            client._ducklake = mock_ducklake

            result = await client.download(mock_file)

            assert result is not None

        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_download_with_dadosgov_client(self, test_db_path):
        from unittest.mock import AsyncMock, MagicMock, patch

        from pysus.api.extensions import ExtensionFactory

        client = PySUS(db_path=test_db_path)

        mock_local = MagicMock()
        mock_local.path.exists.return_value = False

        mock_file = MagicMock()
        mock_file.client.name = "dadosgov"
        mock_file.size = 1000
        mock_file.path = test_db_path.parent / "remote.dadosgov"
        mock_file.basename = "remote.dadosgov"
        mock_file.year = None
        mock_file.month = None
        mock_file.state = None
        mock_file.group = MagicMock()
        mock_file.group.name = None

        with (
            patch.object(
                client, "get_local_file", new=AsyncMock(return_value=mock_local)
            ),
            patch.object(
                client,
                "_get_dest_path",
                return_value=test_db_path.parent / "test.dadosgov",
            ),
            patch.object(client, "_update_state", new=AsyncMock()),
            patch.object(
                ExtensionFactory,
                "instantiate",
                new_callable=AsyncMock,
                return_value=mock_local,
            ),
        ):
            mock_dadosgov = AsyncMock()
            mock_dadosgov.download = AsyncMock()
            client._dadosgov = mock_dadosgov

            result = await client.download(mock_file, token="test_token")

            assert result is not None

        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_download_with_unknown_client_raises_valueerror(
        self, test_db_path
    ):
        from unittest.mock import AsyncMock, MagicMock, patch

        client = PySUS(db_path=test_db_path)

        mock_local = MagicMock()
        mock_local.path.exists.return_value = False

        mock_file = MagicMock()
        mock_file.client.name = "unknown"
        mock_file.size = 1000
        mock_file.basename = "test.unknown"
        mock_file.path = test_db_path.parent / "test.unknown"

        with (
            patch.object(
                client, "get_local_file", new=AsyncMock(return_value=mock_local)
            ),
            patch.object(
                client,
                "_get_dest_path",
                return_value=test_db_path.parent / "test.unknown",
            ),
            patch.object(client, "_update_state", new=AsyncMock()),
        ):
            with pytest.raises(
                DownloadError,
                match=(
                    "Unexpected error downloading test.unknown:"
                    " No download logic for client: unknown"
                ),
            ):
                await client.download(mock_file)

        await client.__aexit__(None, None, None)


class TestDownloadToParquet:
    @pytest.mark.asyncio
    async def test_download_to_parquet_success(self, test_db_path, tmp_path):
        from unittest.mock import AsyncMock, MagicMock, patch

        client = PySUS(db_path=test_db_path)

        original_path = tmp_path / "test.dbc"
        original_path.write_text("dummy content")

        parquet_path = tmp_path / "test.parquet"

        mock_parquet_file = MagicMock()
        mock_parquet_file.path = parquet_path

        mock_local_file = MagicMock()
        mock_local_file.path = original_path
        mock_local_file.to_parquet = AsyncMock(return_value=mock_parquet_file)

        mock_file = MagicMock()
        mock_file.path = "/remote/test.dbc"
        mock_file.client.name = "ftp"
        mock_file.year = 2024
        mock_file.month = 1
        mock_file.state = "SP"
        mock_file.group = MagicMock()
        mock_file.group.name = "DC"

        with (
            patch.object(
                client, "download", new=AsyncMock(return_value=mock_local_file)
            ),
            patch.object(client, "_update_state", new=AsyncMock()),
            patch.object(client, "_delete_record", new=AsyncMock()),
        ):
            result = await client.download_to_parquet(mock_file)

            assert result == mock_parquet_file
            assert result.add_dv is True
            mock_local_file.to_parquet.assert_awaited_once()

        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_download_to_parquet_not_tabular_raises(self, test_db_path):
        from unittest.mock import AsyncMock, MagicMock, patch

        client = PySUS(db_path=test_db_path)

        mock_local_file = MagicMock(spec=[])

        mock_file = MagicMock()
        mock_file.path = "/remote/test.dbc"
        mock_file.client.name = "ftp"

        with patch.object(
            client, "download", new=AsyncMock(return_value=mock_local_file)
        ):
            with pytest.raises(
                FormatError, match="can't be converted to Parquet"
            ):
                await client.download_to_parquet(mock_file)

        await client.__aexit__(None, None, None)


class TestReadParquet:
    def test_read_parquet_single_path(self, tmp_path):
        import pandas as pd

        parquet_file = tmp_path / "test.parquet"
        pd.DataFrame({"a": [1, 2], "b": [3, 4]}).to_parquet(parquet_file)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet([parquet_file])
        df = result.df()

        assert len(df) == 2
        assert "a" in df.columns
        assert "b" in df.columns

    def test_read_parquet_union_mode(self, tmp_path):
        import pandas as pd

        parquet1 = tmp_path / "test1.parquet"
        parquet2 = tmp_path / "test2.parquet"

        pd.DataFrame({"a": [1], "b": [2]}).to_parquet(parquet1)
        pd.DataFrame({"a": [3], "c": [4]}).to_parquet(parquet2)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet([parquet1, parquet2], mode="union")
        df = result.df()

        assert len(df) == 2
        assert "a" in df.columns
        assert "b" in df.columns
        assert "c" in df.columns

    def test_read_parquet_intersection_mode(self, tmp_path):
        import pandas as pd

        parquet1 = tmp_path / "test1.parquet"
        parquet2 = tmp_path / "test2.parquet"

        pd.DataFrame({"a": [1], "b": [2]}).to_parquet(parquet1)
        pd.DataFrame({"a": [3], "c": [4]}).to_parquet(parquet2)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet([parquet1, parquet2], mode="intersection")
        df = result.df()

        assert len(df) == 2
        assert list(df.columns) == ["a"]

    def test_read_parquet_intersection_no_common_columns(self, tmp_path):
        import duckdb
        import pandas as pd

        parquet1 = tmp_path / "test1.parquet"
        parquet2 = tmp_path / "test2.parquet"

        pd.DataFrame({"a": [1], "b": [2]}).to_parquet(parquet1)
        pd.DataFrame({"c": [3], "d": [4]}).to_parquet(parquet2)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")

        original_execute = duckdb.execute

        def side_effect(sql, *args, **kwargs):
            if sql == "SELECT * WHERE 1=0":
                result = MagicMock()
                result.description = []
                result.df.return_value = pd.DataFrame()
                result.fetchall.return_value = []
                return result
            return original_execute(sql, *args, **kwargs)

        with patch.object(duckdb, "execute", side_effect=side_effect):
            result = client.read_parquet(
                [parquet1, parquet2], mode="intersection"
            )
            df = result.df()
            assert len(df) == 0

    def test_read_parquet_strict_mode_matching_schemas(self, tmp_path):
        import pandas as pd

        parquet1 = tmp_path / "test1.parquet"
        parquet2 = tmp_path / "test2.parquet"

        pd.DataFrame({"a": [1], "b": [2]}).to_parquet(parquet1)
        pd.DataFrame({"a": [3], "b": [4]}).to_parquet(parquet2)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet([parquet1, parquet2], mode="strict")
        df = result.df()

        assert len(df) == 2

    def test_read_parquet_strict_mode_mismatching_schemas(self, tmp_path):
        import pandas as pd

        parquet1 = tmp_path / "test1.parquet"
        parquet2 = tmp_path / "test2.parquet"

        pd.DataFrame({"a": [1], "b": [2]}).to_parquet(parquet1)
        pd.DataFrame({"a": [3], "c": [4]}).to_parquet(parquet2)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")

        with pytest.raises(ValidationError, match="Schema mismatch"):
            client.read_parquet([parquet1, parquet2], mode="strict")

    def test_read_parquet_with_sql(self, tmp_path):
        import pandas as pd

        parquet_file = tmp_path / "test.parquet"
        pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).to_parquet(parquet_file)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet(
            [parquet_file], sql="SELECT a FROM t WHERE a > 1"
        )
        df = result.df()

        assert len(df) == 2
        assert list(df.columns) == ["a"]

    def test_read_parquet_sql_not_select(self, tmp_path):
        import pandas as pd

        parquet_file = tmp_path / "test.parquet"
        pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]}).to_parquet(
            parquet_file
        )

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet([parquet_file], sql="a + b AS c")
        df = result.df()

        assert list(df.columns) == ["c"]

    def test_read_parquet_no_paths_raises(self, tmp_path):
        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")

        with pytest.raises(ValidationError, match="No paths provided"):
            client.read_parquet([])

    def test_read_parquet_add_dv_applies_verification_digit(self, tmp_path):
        import pandas as pd

        parquet_file = tmp_path / "test.parquet"
        df = pd.DataFrame({"ID_MUNICIP": ["261160", "530010"], "value": [1, 2]})
        df.to_parquet(parquet_file)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet([parquet_file], add_dv=True)
        out = result.df()

        assert out["ID_MUNICIP"].iloc[0] == "2611606"
        assert out["ID_MUNICIP"].iloc[1] == "5300108"

    def test_read_parquet_add_dv_skips_no_geocode_columns(self, tmp_path):
        import pandas as pd

        parquet_file = tmp_path / "test.parquet"
        df = pd.DataFrame({"DT_NOTIFIC": ["20230101"], "value": [1]})
        df.to_parquet(parquet_file)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet([parquet_file], add_dv=True)
        out = result.df()

        assert list(out.columns) == ["DT_NOTIFIC", "value"]

    def test_read_parquet_add_dv_false_returns_raw(self, tmp_path):
        import pandas as pd

        parquet_file = tmp_path / "test.parquet"
        df = pd.DataFrame({"ID_MUNICIP": ["261160"], "value": [1]})
        df.to_parquet(parquet_file)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet([parquet_file], add_dv=False)

        from duckdb import DuckDBPyConnection

        assert isinstance(result, DuckDBPyConnection)
        out = result.df()
        assert out["ID_MUNICIP"].iloc[0] == "261160"

    def test_read_parquet_add_dv_create_function_exception(self, tmp_path):
        import duckdb
        import pandas as pd

        parquet_file = tmp_path / "test.parquet"
        df = pd.DataFrame({"ID_MUNICIP": ["261160"], "value": [1]})
        df.to_parquet(parquet_file)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")

        with patch.object(
            duckdb,
            "create_function",
            side_effect=duckdb.NotImplementedException(),
        ):
            result = client.read_parquet([parquet_file], add_dv=True)
            out = result.df()
            assert out["ID_MUNICIP"].iloc[0] == "2611606"


class TestPySUSGetMethods:
    @pytest.mark.asyncio
    async def test_get_dadosgov(self, test_db_path):
        from pysus.api.dadosgov import DadosGovClient

        client = PySUS(db_path=test_db_path)
        assert client._dadosgov is None

        with patch.object(
            DadosGovClient, "connect", new_callable=AsyncMock
        ) as mock_connect:
            result = await client.get_dadosgov("test_token")
            assert result is not None
            assert client._dadosgov is not None
            mock_connect.assert_called_once()

        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_get_ftp(self, test_db_path):
        from pysus.api.ftp import FTPClient

        client = PySUS(db_path=test_db_path)
        assert client._ftp is None

        with patch.object(
            FTPClient, "connect", new_callable=AsyncMock
        ) as mock_connect:
            result = await client.get_ftp()
            assert result is not None
            assert client._ftp is not None
            mock_connect.assert_called_once()

        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_aenter(self, test_db_path):
        from pysus.api.ducklake.client import DuckLake

        client = PySUS(db_path=test_db_path)

        with patch.object(DuckLake, "connect", new_callable=AsyncMock):
            await client.__aenter__()
            assert client._ducklake is not None

        await client.__aexit__(None, None, None)
