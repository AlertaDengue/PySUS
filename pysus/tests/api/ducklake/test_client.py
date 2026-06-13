"""Tests for DuckLake client module."""

import errno
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pysus.api.ducklake.catalog.orm.dataset import File as CatalogFile
from pysus.api.ducklake.catalog.orm.default import Dataset as PerDataset
from pysus.api.ducklake.client import DuckLake, DuckLakeCredentials
from pysus.api.ducklake.models import DuckDataset, File


class TestDuckLakeCredentials:
    def test_credentials_creation(self):
        creds = DuckLakeCredentials(
            access_key="test_key",
            secret_key="test_secret",
        )
        assert creds.access_key.get_secret_value() == "test_key"
        assert creds.secret_key.get_secret_value() == "test_secret"


class TestDuckLake:
    @pytest.mark.asyncio
    async def test_ducklake_init(self):
        client = DuckLake()
        assert client.name == "DuckLake"
        assert client.long_name == "PySUS s3 Client"
        assert client.endpoint == "nbg1.your-objectstorage.com"
        assert client.bucket == "pysus"

    @pytest.mark.asyncio
    async def test_description(self):
        client = DuckLake()
        assert client.description == ""

    @pytest.mark.asyncio
    async def test_ducklake_catalog_path(self, tmp_path):
        with patch("pysus.api.ducklake.client.CACHEPATH", tmp_path):
            client = DuckLake()
            assert (
                client.catalog_path == tmp_path / "ducklake" / "catalog.duckdb"
            )

    @pytest.mark.asyncio
    async def test_ducklake_catalog_url(self):
        client = DuckLake()
        expected = (
            "https://nbg1.your-objectstorage.com/pysus/public/catalog.duckdb"
        )
        assert client._catalog_url == expected

    @pytest.mark.asyncio
    async def test_is_authenticated_false_no_credentials(self):
        client = DuckLake()
        assert client._is_authenticated is False

    @pytest.mark.asyncio
    async def test_is_authenticated_with_credentials(self):
        client = DuckLake()
        with patch.object(client, "_download_catalog"):
            await client.login(access_key="key", secret_key="secret")
        assert client._is_authenticated is True

    @pytest.mark.asyncio
    async def test_login_sets_credentials(self):
        client = DuckLake()
        with patch.object(client, "_download_catalog"):
            await client.login(access_key="key", secret_key="secret")
        assert client.credentials is not None

    @pytest.mark.asyncio
    async def test_login_creates_s3_client(self):
        client = DuckLake()
        with patch.object(client, "_download_catalog"):
            await client.login(access_key="key", secret_key="secret")
        assert client._s3_client is not None

    @pytest.mark.asyncio
    async def test_login_clears_credentials(self):
        client = DuckLake()
        client.credentials = DuckLakeCredentials(
            access_key="test_key",
            secret_key="test_secret",
        )
        with patch.object(client, "_download_catalog"):
            await client.login()
        assert client.credentials is None
        assert client._s3_client is None

    @pytest.mark.asyncio
    async def test_close_clears_state(self):
        client = DuckLake()
        client._engine = MagicMock()
        with patch(
            "pysus.api.ducklake.client.to_thread.run_sync",
            side_effect=lambda fn, *a, **kw: fn(),
        ):
            await client.close()
        assert client._engine is None
        assert client._Session is None
        assert client._s3_client is None

    @pytest.mark.asyncio
    async def test_close_with_datasets(self):
        client = DuckLake()
        ds = AsyncMock(spec=DuckDataset)
        client._datasets.append(ds)
        await client.close()
        ds.close.assert_awaited_once_with(update_catalog=False)
        assert client._datasets == []

    @pytest.mark.asyncio
    async def test_close_with_update_catalog(self):
        client = DuckLake()
        ds = AsyncMock(spec=DuckDataset)
        client._datasets.append(ds)
        with patch.object(client, "_upload_catalog") as mock_upload:
            await client.close(update_catalog=True)
            mock_upload.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_s3_client_requires_credentials(self):
        client = DuckLake()
        with pytest.raises(ConnectionError):
            client._get_s3_client()

    @pytest.mark.asyncio
    async def test_upload_catalog_requires_auth(self):
        client = DuckLake()
        with pytest.raises(PermissionError):
            await client._upload_catalog()


class TestDuckLakeDatasets:
    @pytest.mark.asyncio
    async def test_datasets_creates_session_and_returns_duckdatasets(
        self, tmp_path
    ):
        with patch("pysus.api.ducklake.client.CACHEPATH", tmp_path):
            client = DuckLake()

        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session

        record = PerDataset(name="sinan", long_name="SINAN", description="Test")

        mock_session.query.return_value.all.return_value = [record]
        client._Session = MagicMock(return_value=mock_session)

        def run_sync(fn, *args, **kwargs):
            return fn()

        with patch(
            "pysus.api.ducklake.client.to_thread.run_sync",
            side_effect=run_sync,
        ):
            result = await client.datasets()

        assert len(result) == 1
        assert isinstance(result[0], DuckDataset)
        assert result[0].record.name == "sinan"

    @pytest.mark.asyncio
    async def test_datasets_connects_if_no_session(self, tmp_path):
        with patch("pysus.api.ducklake.client.CACHEPATH", tmp_path):
            client = DuckLake()

        assert client._Session is None

        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.all.return_value = []

        async def _connect(*args, **kwargs):
            client._Session = MagicMock(return_value=mock_session)

        with patch.object(
            DuckLake, "connect", new=AsyncMock(side_effect=_connect)
        ):

            def run_sync(fn, *args, **kwargs):
                return fn()

            with patch(
                "pysus.api.ducklake.client.to_thread.run_sync",
                side_effect=run_sync,
            ):
                await client.datasets()


class TestDuckLakeSetupEngine:
    def test_setup_engine_has_pysus_schema(self):
        with patch("pysus.api.ducklake.client.create_engine") as mock_create:
            mock_engine = MagicMock()
            mock_conn = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            mock_create.return_value = mock_engine

            mock_conn.exec_driver_sql().fetchone.return_value = (1,)

            client = DuckLake()
            result = client._setup_engine()

            calls = [str(c) for c in mock_conn.exec_driver_sql.call_args_list]
            assert any(
                "SET search_path" in c and "pysus,main" in c for c in calls
            )
            assert result is mock_engine

    def test_setup_engine_no_pysus_schema(self):
        with patch("pysus.api.ducklake.client.create_engine") as mock_create:
            mock_engine = MagicMock()
            mock_conn = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            mock_create.return_value = mock_engine

            mock_conn.exec_driver_sql().fetchone.return_value = None

            client = DuckLake()
            result = client._setup_engine()

            calls = [str(c) for c in mock_conn.exec_driver_sql.call_args_list]
            assert any("SET search_path" in c and "'main'" in c for c in calls)
            assert result is mock_engine

    def test_setup_engine_with_credentials(self):
        with patch("pysus.api.ducklake.client.create_engine") as mock_create:
            mock_engine = MagicMock()
            mock_conn = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            mock_create.return_value = mock_engine

            mock_conn.exec_driver_sql().fetchone.return_value = None

            client = DuckLake(
                credentials=DuckLakeCredentials(
                    access_key="ak", secret_key="sk"
                )
            )
            client._setup_engine()

            calls = [str(c) for c in mock_conn.exec_driver_sql.call_args_list]
            s3_access = any(
                "s3_access_key_id" in c and "ak" in c for c in calls
            )
            s3_secret = any(
                "s3_secret_access_key" in c and "sk" in c for c in calls
            )
            assert s3_access
            assert s3_secret


class TestDuckLakeConnect:
    @pytest.mark.asyncio
    async def test_connect_already_connected_returns_early(self):
        client = DuckLake()
        client._engine = MagicMock()
        client._Session = MagicMock()
        with patch.object(client, "_download_catalog") as mock_dl:
            await client.connect()
            mock_dl.assert_not_called()

    @pytest.mark.asyncio
    async def test_connect_creates_session_if_missing(self):
        client = DuckLake()
        client._engine = MagicMock()
        client._Session = None
        with patch.object(client, "_download_catalog") as mock_dl:
            await client.connect()
            assert client._Session is not None
            mock_dl.assert_not_called()

    @pytest.mark.asyncio
    async def test_connect_downloads_and_sets_up_engine(self, tmp_path):
        with patch("pysus.api.ducklake.client.CACHEPATH", tmp_path):
            client = DuckLake()

        client._engine = None

        def run_sync(fn, *args, **kwargs):
            return fn()

        with patch.object(client, "_download_catalog") as mock_dl:
            with patch(
                "pysus.api.ducklake.client.to_thread.run_sync",
                side_effect=run_sync,
            ):
                with patch.object(
                    client, "_setup_engine", return_value=MagicMock()
                ):
                    await client.connect()
                    mock_dl.assert_awaited_once_with(
                        client._catalog_local,
                        client._catalog_remote,
                    )
                    assert client._Session is not None
                    assert client._engine is not None


class TestDuckLakeDownload:
    @pytest.mark.asyncio
    async def test_download_retry_then_success(self, tmp_path):
        client = DuckLake()
        local_path = tmp_path / "test.db"
        remote_path = "public/test.db"

        class FailingAsyncIter:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise OSError("Connection dropped")

        mock_client = MagicMock()
        mock_client.__aenter__.return_value = mock_client
        httpx_patcher = patch(
            "pysus.api.ducklake.client.httpx.AsyncClient",
            return_value=mock_client,
        )
        sleep_patcher = patch(
            "pysus.api.ducklake.client.sleep", new_callable=AsyncMock
        )

        first_stream_cm = MagicMock()
        first_resp = MagicMock()
        first_stream_cm.__aenter__.return_value = first_resp
        first_resp.raise_for_status = MagicMock()
        first_resp.headers.get.return_value = "4"
        first_resp.aiter_bytes.return_value = FailingAsyncIter()

        second_stream_cm = MagicMock()

        async def success_iter():
            yield b"data"

        second_resp = MagicMock()
        second_stream_cm.__aenter__.return_value = second_resp
        second_resp.raise_for_status = MagicMock()
        second_resp.headers.get.return_value = "4"
        second_resp.aiter_bytes.return_value = success_iter()

        mock_client.stream.side_effect = [first_stream_cm, second_stream_cm]

        with httpx_patcher, sleep_patcher as mock_sleep:
            await client.download(remote_path, local_path)

        assert local_path.exists()
        assert local_path.read_bytes() == b"data"
        assert mock_client.stream.call_count == 2
        mock_sleep.assert_awaited_once_with(1)

    @pytest.mark.asyncio
    async def test_download_retry_exhausted_raises(self, tmp_path):
        client = DuckLake()
        local_path = tmp_path / "test.db"
        remote_path = "public/test.db"

        class FailingAsyncIter:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise OSError("Connection dropped")

        mock_client = MagicMock()
        mock_client.__aenter__.return_value = mock_client
        httpx_patcher = patch(
            "pysus.api.ducklake.client.httpx.AsyncClient",
            return_value=mock_client,
        )
        sleep_patcher = patch(
            "pysus.api.ducklake.client.sleep", new_callable=AsyncMock
        )

        stream_cm = MagicMock()
        resp = MagicMock()
        stream_cm.__aenter__.return_value = resp
        resp.raise_for_status = MagicMock()
        resp.headers.get.return_value = "4"
        resp.aiter_bytes.return_value = FailingAsyncIter()

        mock_client.stream.return_value = stream_cm

        with httpx_patcher, sleep_patcher as mock_sleep:
            with pytest.raises(OSError, match="Connection dropped"):
                await client.download(remote_path, local_path)

        assert mock_client.stream.call_count == 5
        assert mock_sleep.await_count == 4

    @pytest.mark.asyncio
    async def test_download_with_callback(self, tmp_path):
        client = DuckLake()
        local_path = tmp_path / "test.db"
        remote_path = "public/test.db"

        mock_client = MagicMock()
        mock_client.__aenter__.return_value = mock_client

        stream_cm = MagicMock()

        async def success_iter():
            yield b"hello"
            yield b"world"

        resp = MagicMock()
        stream_cm.__aenter__.return_value = resp
        resp.raise_for_status = MagicMock()
        resp.headers.get.return_value = "10"
        resp.aiter_bytes.return_value = success_iter()

        mock_client.stream.return_value = stream_cm

        callback = MagicMock()

        with patch(
            "pysus.api.ducklake.client.httpx.AsyncClient",
            return_value=mock_client,
        ):
            await client.download(remote_path, local_path, callback=callback)

        callback.assert_any_call(5, 10)
        callback.assert_any_call(10, 10)


class TestDuckLakeDownloadCatalog:
    @pytest.mark.asyncio
    async def test_download_catalog_size_match_skips_download(self, tmp_path):
        local_path = tmp_path / "catalog.duckdb"
        local_path.write_text("test")
        remote_path = "public/catalog.duckdb"

        client = DuckLake()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.headers = {"content-length": "4"}
        mock_resp.raise_for_status = MagicMock()
        mock_http.head = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__.return_value = mock_http

        with patch(
            "pysus.api.ducklake.client.httpx.AsyncClient",
            return_value=mock_http,
        ):
            with patch.object(client, "_download") as mock_dl:
                await client._download_catalog(local_path, remote_path)
                mock_dl.assert_not_called()

    @pytest.mark.asyncio
    async def test_download_catalog_size_mismatch_downloads(self, tmp_path):
        local_path = tmp_path / "catalog.duckdb"
        local_path.write_text("test")
        remote_path = "public/catalog.duckdb"

        client = DuckLake()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.headers = {"content-length": "100"}
        mock_resp.raise_for_status = MagicMock()
        mock_http.head = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__.return_value = mock_http

        with patch(
            "pysus.api.ducklake.client.httpx.AsyncClient",
            return_value=mock_http,
        ):
            with patch.object(client, "_download") as mock_dl:
                await client._download_catalog(local_path, remote_path)
                mock_dl.assert_awaited_once_with(remote_path, local_path)

    @pytest.mark.asyncio
    async def test_download_catalog_local_not_exists(self, tmp_path):
        local_path = tmp_path / "catalog.duckdb"
        remote_path = "public/catalog.duckdb"

        client = DuckLake()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.headers = {"content-length": "100"}
        mock_resp.raise_for_status = MagicMock()
        mock_http.head = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__.return_value = mock_http

        with patch(
            "pysus.api.ducklake.client.httpx.AsyncClient",
            return_value=mock_http,
        ):
            with patch.object(client, "_download") as mock_dl:
                await client._download_catalog(local_path, remote_path)
                mock_dl.assert_awaited_once_with(remote_path, local_path)

    @pytest.mark.asyncio
    async def test_download_catalog_head_fails(self, tmp_path):
        local_path = tmp_path / "catalog.duckdb"
        remote_path = "public/catalog.duckdb"

        client = DuckLake()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.headers = {}
        mock_http.head = AsyncMock(side_effect=Exception("HEAD failed"))
        mock_http.__aenter__.return_value = mock_http

        with patch(
            "pysus.api.ducklake.client.httpx.AsyncClient",
            return_value=mock_http,
        ):
            with patch.object(client, "_download") as mock_dl:
                await client._download_catalog(local_path, remote_path)
                mock_dl.assert_awaited_once_with(remote_path, local_path)

    @pytest.mark.asyncio
    async def test_download_catalog_head_no_content_length(self, tmp_path):
        local_path = tmp_path / "catalog.duckdb"
        local_path.write_text("test")
        remote_path = "public/catalog.duckdb"

        client = DuckLake()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.headers = {}
        mock_resp.raise_for_status = MagicMock()
        mock_http.head = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__.return_value = mock_http

        with patch(
            "pysus.api.ducklake.client.httpx.AsyncClient",
            return_value=mock_http,
        ):
            with patch.object(client, "_download") as mock_dl:
                await client._download_catalog(local_path, remote_path)
                mock_dl.assert_awaited_once_with(remote_path, local_path)

    @pytest.mark.asyncio
    async def test_download_catalog_oserror_on_local_stat(self, tmp_path):
        local_path = tmp_path / "catalog.duckdb"
        local_path.write_text("test")
        remote_path = "public/catalog.duckdb"

        client = DuckLake()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.headers = {"content-length": "999"}
        mock_resp.raise_for_status = MagicMock()
        mock_http.head = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__.return_value = mock_http

        stat_call_count = 0
        original_stat = type(local_path).stat

        def broken_stat(self, *args, **kwargs):
            nonlocal stat_call_count
            stat_call_count += 1
            if stat_call_count == 2:
                raise OSError(errno.EACCES, "permission denied")
            return original_stat(self, *args, **kwargs)

        with patch.object(type(local_path), "stat", broken_stat):
            with patch(
                "pysus.api.ducklake.client.httpx.AsyncClient",
                return_value=mock_http,
            ):
                with patch.object(client, "_download") as mock_dl:
                    await client._download_catalog(local_path, remote_path)
                    mock_dl.assert_awaited_once_with(remote_path, local_path)


class TestDuckLakeDownloadFile:
    @pytest.mark.asyncio
    async def test_download_file_invalid_type_raises(self):
        client = DuckLake()
        with pytest.raises(
            ValueError, match="FTP File was not properly instantiated"
        ):
            await client.download(
                "not-a-file",
                Path("/tmp/test"),
            )  # type: ignore

    @pytest.mark.asyncio
    async def test_download_file_valid(self, tmp_path):
        client = DuckLake()

        record = CatalogFile(
            path="remote/path/file.csv",
            type="csv",
            size=100,
            rows=10,
            modified=datetime.now(),
            origin_size=100,
            origin_path="remote/path/file.csv",
        )

        dataset = MagicMock(spec=DuckDataset)
        f = File(dataset=dataset, record=record)  # type: ignore

        output = tmp_path / "output.csv"
        with patch.object(client, "_download") as mock_dl:
            result = await client.download(f, output)
            mock_dl.assert_awaited_once_with(record.path, output, callback=None)
            assert result == output


class TestDuckLakeUploadCatalog:
    @pytest.mark.asyncio
    async def test_upload_catalog_with_datasets(self, tmp_path):
        client = DuckLake(
            credentials=DuckLakeCredentials(access_key="ak", secret_key="sk")
        )
        client._s3_client = MagicMock()

        ds = AsyncMock(spec=DuckDataset)
        local_db = tmp_path / "catalog_test.duckdb"
        local_db.write_text("data")
        ds._catalog_local = local_db
        ds._catalog_name = "catalog_test.duckdb"

        with patch.object(
            DuckLake, "datasets", new=AsyncMock(return_value=[ds])
        ):
            await client._upload_catalog()
            client._s3_client.upload_file.assert_called_once_with(
                str(local_db), client.bucket, ds._catalog_name
            )

    @pytest.mark.asyncio
    async def test_upload_catalog_skips_missing_local(self, tmp_path):
        client = DuckLake(
            credentials=DuckLakeCredentials(access_key="ak", secret_key="sk")
        )
        client._s3_client = MagicMock()

        ds = AsyncMock(spec=DuckDataset)
        nonexistent = tmp_path / "nonexistent.duckdb"
        ds._catalog_local = nonexistent
        ds._catalog_name = "catalog_test.duckdb"

        with patch.object(
            DuckLake, "datasets", new=AsyncMock(return_value=[ds])
        ):
            await client._upload_catalog()
            client._s3_client.upload_file.assert_not_called()
