"""Tests for DuckLake client module."""

from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pysus.api.ducklake.catalog.adapters import CatalogAdapter, DatasetAdapter
from pysus.api.ducklake.catalog.orm.dataset import File as CatalogFile
from pysus.api.ducklake.catalog.orm.default import Dataset as PerDataset
from pysus.api.ducklake.client import DuckLake, DuckLakeCredentials
from pysus.api.ducklake.models import DuckDataset, File
from pysus.api.errors import ValidationError


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
        assert client.credentials is None
        assert client.update_on_close is False
        assert isinstance(client._catalog_adap, CatalogAdapter)
        assert client._datasets == []

    @pytest.mark.asyncio
    async def test_description(self):
        client = DuckLake()
        assert client.description == ""

    @pytest.mark.asyncio
    async def test_ducklake_catalog_path(self):
        with patch("pathlib.Path.mkdir"):
            client = DuckLake()
        assert isinstance(client.catalog_path, Path)
        assert client.catalog_path.name == "catalog.duckdb"

    @pytest.mark.asyncio
    async def test_is_authenticated_without_credentials(self):
        client = DuckLake()
        assert client.credentials is None

    @pytest.mark.asyncio
    async def test_login_sets_credentials(self):
        client = DuckLake()
        client._catalog_adap = AsyncMock()
        client._columns_adap = AsyncMock()
        await client.login(access_key="key", secret_key="secret")
        assert client.credentials is not None

    @pytest.mark.asyncio
    async def test_close_with_datasets(self):
        client = DuckLake()
        client._catalog_adap = AsyncMock()
        client._columns_adap = AsyncMock()
        ds = AsyncMock(spec=DuckDataset)
        client._datasets.append(ds)
        await client.close()
        ds.close.assert_awaited_once_with(update_catalog=False)

    @pytest.mark.asyncio
    async def test_close_with_update_catalog(self):
        client = DuckLake()
        client._catalog_adap = AsyncMock()
        client._columns_adap = AsyncMock()
        ds = AsyncMock(spec=DuckDataset)
        client._datasets.append(ds)
        await client.close(update_catalog=True)
        ds.close.assert_awaited_once_with(update_catalog=True)


class TestDuckLakeDatasets:
    @pytest.mark.asyncio
    async def test_datasets_returns_duckdatasets(self, tmp_path):
        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            with patch("pathlib.Path.mkdir"):
                client = DuckLake()

        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session

        record = PerDataset(name="sinan", long_name="SINAN", description="Test")
        record.id = 1

        mock_session.query.return_value.all.return_value = [record]

        mock_catalog_adap = MagicMock()
        mock_catalog_adap.__aenter__.return_value = mock_catalog_adap
        mock_catalog_adap.__aexit__ = AsyncMock()
        mock_catalog_adap.get_session.return_value = mock_session
        client._catalog_adap = mock_catalog_adap

        def run_sync(fn, *args, **kwargs):
            return fn()

        with patch(
            "pysus.api.ducklake.client.to_thread.run_sync",
            side_effect=run_sync,
        ):
            with patch("pathlib.Path.mkdir"):
                result = await client.datasets()

        assert len(result) == 1
        assert isinstance(result[0], DuckDataset)
        assert result[0].record.name == "sinan"

    @pytest.mark.asyncio
    async def test_datasets_connects_if_no_session(self, tmp_path):
        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            with patch("pathlib.Path.mkdir"):
                client = DuckLake()

        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.all.return_value = []

        mock_catalog_adap = MagicMock()
        mock_catalog_adap.__aenter__.return_value = mock_catalog_adap
        mock_catalog_adap.__aexit__ = AsyncMock()
        mock_catalog_adap.get_session.return_value = mock_session
        client._catalog_adap = mock_catalog_adap

        def run_sync(fn, *args, **kwargs):
            return fn()

        with patch(
            "pysus.api.ducklake.client.to_thread.run_sync",
            side_effect=run_sync,
        ):
            with patch("pathlib.Path.mkdir"):
                await client.datasets()


class TestDuckLakeConnect:
    @pytest.mark.asyncio
    async def test_connect_delegates_to_adapters(self):
        client = DuckLake()
        client._catalog_adap = AsyncMock()
        client._columns_adap = AsyncMock()
        await client.connect()
        client._catalog_adap.connect.assert_awaited_once_with(
            force=False, callback=None
        )
        client._columns_adap.connect.assert_awaited_once_with(
            force=False, callback=None
        )

    @pytest.mark.asyncio
    async def test_connect_force(self):
        client = DuckLake()
        client._catalog_adap = AsyncMock()
        client._columns_adap = AsyncMock()
        await client.connect(force=True)
        client._catalog_adap.connect.assert_awaited_once_with(
            force=True, callback=None
        )
        client._columns_adap.connect.assert_awaited_once_with(
            force=True, callback=None
        )


class TestDuckLakeDownload:
    @pytest.mark.asyncio
    async def test_download_retry_then_success(self, tmp_path):
        local_path = tmp_path / "test.db"

        class FailingAsyncIter:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise OSError("Connection dropped")

        mock_http = MagicMock()
        mock_http.__aenter__.return_value = mock_http
        httpx_patcher = patch(
            "pysus.api.ducklake.functional.httpx.AsyncClient",
            return_value=mock_http,
        )
        sleep_patcher = patch(
            "pysus.api.ducklake.functional.sleep", new_callable=AsyncMock
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

        mock_http.stream.side_effect = [first_stream_cm, second_stream_cm]

        with httpx_patcher, sleep_patcher as mock_sleep:
            with patch(
                "pysus.api.ducklake.functional.to_thread.run_sync",
                side_effect=lambda fn, *a, **kw: fn(*a, **kw),
            ):
                from pysus.api.ducklake.functional import download_http

                await download_http("public/test.db", local_path)

        assert local_path.exists()
        assert local_path.read_bytes() == b"data"
        assert mock_http.stream.call_count == 2
        mock_sleep.assert_awaited_once_with(2)

    @pytest.mark.asyncio
    async def test_download_retry_exhausted_raises(self, tmp_path):
        local_path = tmp_path / "test.db"

        class FailingAsyncIter:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise OSError("Connection dropped")

        mock_http = MagicMock()
        mock_http.__aenter__.return_value = mock_http
        httpx_patcher = patch(
            "pysus.api.ducklake.functional.httpx.AsyncClient",
            return_value=mock_http,
        )
        sleep_patcher = patch(
            "pysus.api.ducklake.functional.sleep", new_callable=AsyncMock
        )

        stream_cm = MagicMock()
        resp = MagicMock()
        stream_cm.__aenter__.return_value = resp
        resp.raise_for_status = MagicMock()
        resp.headers.get.return_value = "4"
        resp.aiter_bytes.return_value = FailingAsyncIter()

        mock_http.stream.return_value = stream_cm

        with httpx_patcher, sleep_patcher as mock_sleep:
            with pytest.raises(OSError, match="Connection dropped"):
                with patch(
                    "pysus.api.ducklake.functional.to_thread.run_sync",
                    side_effect=lambda fn, *a, **kw: fn(*a, **kw),
                ):
                    from pysus.api.ducklake.functional import download_http

                    await download_http("public/test.db", local_path)

        assert mock_http.stream.call_count == 5
        assert mock_sleep.await_count == 4

    @pytest.mark.asyncio
    async def test_download_with_callback(self, tmp_path):
        local_path = tmp_path / "test.db"

        mock_http = MagicMock()
        mock_http.__aenter__.return_value = mock_http

        stream_cm = MagicMock()

        async def success_iter():
            yield b"hello"
            yield b"world"

        resp = MagicMock()
        stream_cm.__aenter__.return_value = resp
        resp.raise_for_status = MagicMock()
        resp.headers.get.return_value = "10"
        resp.aiter_bytes.return_value = success_iter()

        mock_http.stream.return_value = stream_cm

        callback = MagicMock()

        with patch(
            "pysus.api.ducklake.functional.httpx.AsyncClient",
            return_value=mock_http,
        ):
            with patch(
                "pysus.api.ducklake.functional.to_thread.run_sync",
                side_effect=lambda fn, *a, **kw: fn(*a, **kw),
            ):
                from pysus.api.ducklake.functional import download_http

                await download_http(
                    "public/test.db", local_path, callback=callback
                )

        callback.assert_any_call(5, 10)
        callback.assert_any_call(10, 10)


class TestDuckLakeDownloadCatalog:
    @pytest.mark.asyncio
    async def test_download_catalog_size_match_skips_download(self, tmp_path):
        local_path = tmp_path / "catalog.duckdb"
        local_path.write_text("test")

        client = DuckLake()
        client._catalog_adap.db_local = local_path
        client._catalog_adap.db_remote = Path("public/catalog.duckdb")

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.headers = {"content-length": "4"}
        mock_resp.raise_for_status = MagicMock()
        mock_http.head = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__.return_value = mock_http

        with patch(
            "pysus.api.ducklake.catalog.adapters.httpx.AsyncClient",
            return_value=mock_http,
        ):
            with patch(
                "pysus.api.ducklake.catalog.adapters.download_http",
                new_callable=AsyncMock,
            ) as mock_dl:
                await client._catalog_adap._download_catalog(
                    local_path, "public/catalog.duckdb"
                )
                mock_dl.assert_not_called()

    @pytest.mark.asyncio
    async def test_download_catalog_size_mismatch_downloads(self, tmp_path):
        local_path = tmp_path / "catalog.duckdb"
        local_path.write_text("test")

        client = DuckLake()
        client._catalog_adap.db_local = local_path
        client._catalog_adap.db_remote = Path("public/catalog.duckdb")

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.headers = {"content-length": "100"}
        mock_resp.raise_for_status = MagicMock()
        mock_http.head = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__.return_value = mock_http

        with patch(
            "pysus.api.ducklake.catalog.adapters.httpx.AsyncClient",
            return_value=mock_http,
        ):
            with patch(
                "pysus.api.ducklake.catalog.adapters.download_http",
                new_callable=AsyncMock,
            ) as mock_dl:
                await client._catalog_adap._download_catalog(
                    local_path, "public/catalog.duckdb"
                )
                mock_dl.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_download_catalog_local_not_exists(self, tmp_path):
        local_path = tmp_path / "catalog.duckdb"

        client = DuckLake()
        client._catalog_adap.db_local = local_path
        client._catalog_adap.db_remote = Path("public/catalog.duckdb")

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.headers = {"content-length": "100"}
        mock_resp.raise_for_status = MagicMock()
        mock_http.head = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__.return_value = mock_http

        with patch(
            "pysus.api.ducklake.catalog.adapters.httpx.AsyncClient",
            return_value=mock_http,
        ):
            with patch(
                "pysus.api.ducklake.catalog.adapters.download_http",
                new_callable=AsyncMock,
            ) as mock_dl:
                await client._catalog_adap._download_catalog(
                    local_path, "public/catalog.duckdb"
                )
                mock_dl.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_download_catalog_head_fails(self, tmp_path):
        local_path = tmp_path / "catalog.duckdb"

        client = DuckLake()
        client._catalog_adap.db_local = local_path
        client._catalog_adap.db_remote = Path("public/catalog.duckdb")

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.headers = {}
        mock_http.head = AsyncMock(side_effect=Exception("HEAD failed"))
        mock_http.__aenter__.return_value = mock_http

        with patch(
            "pysus.api.ducklake.catalog.adapters.httpx.AsyncClient",
            return_value=mock_http,
        ):
            with patch(
                "pysus.api.ducklake.catalog.adapters.download_http",
                new_callable=AsyncMock,
            ) as mock_dl:
                await client._catalog_adap._download_catalog(
                    local_path, "public/catalog.duckdb"
                )
                mock_dl.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_download_catalog_head_no_content_length(self, tmp_path):
        local_path = tmp_path / "catalog.duckdb"
        local_path.write_text("test")

        client = DuckLake()
        client._catalog_adap.db_local = local_path
        client._catalog_adap.db_remote = Path("public/catalog.duckdb")

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.headers = {}
        mock_resp.raise_for_status = MagicMock()
        mock_http.head = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__.return_value = mock_http

        with patch(
            "pysus.api.ducklake.catalog.adapters.httpx.AsyncClient",
            return_value=mock_http,
        ):
            with patch(
                "pysus.api.ducklake.catalog.adapters.download_http",
                new_callable=AsyncMock,
            ) as mock_dl:
                await client._catalog_adap._download_catalog(
                    local_path, "public/catalog.duckdb"
                )
                mock_dl.assert_awaited_once()


class TestDuckLakeDownloadFile:
    @pytest.mark.asyncio
    async def test_download_file_invalid_type_raises(self):
        client = DuckLake()
        with pytest.raises(
            ValidationError,
            match="DuckLake File was not properly instantiated",
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
        adapter = MagicMock(spec=DatasetAdapter)
        dataset.border = adapter
        f = File(dataset=dataset, record=record)  # type: ignore

        output = tmp_path / "output.csv"
        with patch(
            "pysus.api.ducklake.client.download_http",
            new_callable=AsyncMock,
        ) as mock_dl:
            result = await client.download(f, output)
            mock_dl.assert_awaited_once_with(
                remote_path=record.path,
                local_path=output,
                callback=None,
            )
            assert result == output


class TestDuckLakeUploadCatalog:
    @pytest.mark.asyncio
    async def test_upload_catalog_no_credentials_raises(self, tmp_path):
        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            with patch("pathlib.Path.mkdir"):
                adapter = CatalogAdapter(credentials=None)
        with pytest.raises(PermissionError, match="Admin credentials"):
            await adapter._upload_catalog()

    @pytest.mark.asyncio
    async def test_upload_catalog_missing_file(self, tmp_path):
        nonexistent = tmp_path / "nonexistent.duckdb"
        creds = DuckLakeCredentials(access_key="ak", secret_key="sk")
        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            with patch("pathlib.Path.mkdir"):
                adapter = CatalogAdapter(credentials=creds)
        adapter.db_local = nonexistent
        with pytest.raises(FileNotFoundError, match="catalog file not found"):
            await adapter._upload_catalog()


class TestDuckLakeLogin:
    @pytest.mark.asyncio
    async def test_login_missing_credentials_raises(self):
        from pysus.api.errors import AuthenticationError

        client = DuckLake()
        with pytest.raises(
            AuthenticationError, match="authentication requires"
        ):
            await client.login()


class TestDuckLakeDestructor:
    def test_del_no_adapters_does_nothing(self):
        client = DuckLake()
        del client._catalog_adap
        del client._columns_adap
        client.__del__()

    def test_del_with_running_loop_creates_task(self):
        import asyncio

        client = DuckLake()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            client.__del__()
        finally:
            loop.close()
            asyncio.set_event_loop(None)


class TestBaseAdapter:
    @pytest.mark.asyncio
    async def test_get_session_before_connect_raises(self, tmp_path):
        """get_session before connect raises CatalogError."""
        from pysus.api.ducklake.catalog.adapters import CatalogAdapter
        from pysus.api.errors import CatalogError

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = CatalogAdapter()
        with pytest.raises(CatalogError, match="not initialized"):
            adapter.get_session()

    @pytest.mark.asyncio
    async def test_sql_before_connect_raises(self, tmp_path):
        """sql before connect raises CatalogError."""
        from pysus.api.ducklake.catalog.adapters import CatalogAdapter
        from pysus.api.errors import CatalogError

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = CatalogAdapter()
        with pytest.raises(CatalogError, match="not initialized"):
            adapter.sql("SELECT 1")

    @pytest.mark.asyncio
    async def test_connect_when_engine_exists_skips(self, tmp_path):
        """connect with existing engine returns immediately."""
        from unittest.mock import MagicMock

        from pysus.api.ducklake.catalog.adapters import CatalogAdapter

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = CatalogAdapter()
        mock_engine = MagicMock()
        adapter._engine = mock_engine
        await adapter.connect()
        assert adapter._engine is mock_engine

    @pytest.mark.asyncio
    async def test_download_catalog_already_exists_same_size_skips(
        self, tmp_path
    ):
        """_download_catalog skips when local size matches remote."""
        from pysus.api.ducklake.catalog.adapters import CatalogAdapter

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = CatalogAdapter()
        local = tmp_path / "test.duckdb"
        local.write_bytes(b"data")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-length": "4"}
        mock_response.raise_for_status = MagicMock()

        async def fake_head(*args, **kwargs):
            return mock_response

        with patch("httpx.AsyncClient.head", new=fake_head):
            await adapter._download_catalog(local, "remote/path", force=False)

    @pytest.mark.asyncio
    async def test_download_catalog_remote_404_returns(self, tmp_path):
        """_download_catalog returns early on 404."""
        from pysus.api.ducklake.catalog.adapters import CatalogAdapter

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = CatalogAdapter()
        local = tmp_path / "test.duckdb"
        local.write_bytes(b"data")

        async def fake_head(*args, **kwargs):
            from httpx import HTTPStatusError

            response = MagicMock()
            response.status_code = 404
            raise HTTPStatusError("404", request=MagicMock(), response=response)

        with patch("httpx.AsyncClient", autospec=True) as mock_client_class:
            mock_client = MagicMock()
            mock_client.head = fake_head
            mock_client_class.return_value.__aenter__.return_value = mock_client
            await adapter._download_catalog(local, "remote/path", force=False)


class TestAdapterConnect:
    @pytest.mark.asyncio
    async def test_connect_force_downloads_and_sets_engine(self, tmp_path):
        from unittest.mock import MagicMock

        from pysus.api.ducklake.catalog.adapters import CatalogAdapter

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = CatalogAdapter()
        local = tmp_path / "ducklake" / "catalog.duckdb"
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text("")
        adapter.db_local = local
        adapter.db_remote = local.parent / "remote.duckdb"

        mock_engine = MagicMock()
        with patch.object(adapter, "setup_engine", return_value=mock_engine):
            with patch.object(
                adapter, "_download_catalog", new_callable=AsyncMock
            ):
                await adapter.connect(force=True)
        assert adapter._engine is mock_engine

    @pytest.mark.asyncio
    async def test_connect_engine_fails_cleanup_and_retry(self, tmp_path):
        from unittest.mock import MagicMock

        from pysus.api.ducklake.catalog.adapters import CatalogAdapter

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = CatalogAdapter()
        local = tmp_path / "ducklake" / "catalog.duckdb"
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text("corrupted")
        adapter.db_local = local

        call_count = [0]

        async def fake_download(*args, **kwargs):
            call_count[0] += 1

        def fake_setup():
            call_count[0]
            if call_count[0] == 1:
                raise RuntimeError("bad db")
            return MagicMock()

        with patch.object(
            adapter, "_download_catalog", side_effect=fake_download
        ):
            with patch.object(adapter, "setup_engine", side_effect=fake_setup):
                await adapter.connect()
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_connect_with_existing_engine_no_session(self, tmp_path):
        from unittest.mock import MagicMock

        from pysus.api.ducklake.catalog.adapters import CatalogAdapter

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = CatalogAdapter()
        mock_engine = MagicMock()
        adapter._engine = mock_engine
        await adapter.connect()
        assert adapter._session_factory is not None


class TestAdapterRemoteUrl:
    def test_remote_url(self, tmp_path):
        from pysus.api.ducklake.catalog.adapters import CatalogAdapter

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = CatalogAdapter()
        url = adapter.remote_url
        assert "catalog.duckdb" in url
        assert url.startswith("https://")


class TestAdapterClose:
    @pytest.mark.asyncio
    async def test_close_disposes_engine(self, tmp_path):
        from unittest.mock import MagicMock

        from pysus.api.ducklake.catalog.adapters import CatalogAdapter

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = CatalogAdapter()
        mock_engine = MagicMock()
        adapter._engine = mock_engine
        await adapter.close()
        mock_engine.dispose.assert_called_once()
        assert adapter._engine is None
        assert adapter._session_factory is None

    def test_destructor_no_engine(self, tmp_path):
        from pysus.api.ducklake.catalog.adapters import CatalogAdapter

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = CatalogAdapter()
        adapter._engine = None
        adapter.__del__()


class TestDatasetAdapter:
    def test_init(self, tmp_path):
        from pysus.api.ducklake.catalog.adapters import DatasetAdapter

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = DatasetAdapter(name="sinan", dataset_id=1)
        assert adapter.dataset_name == "sinan"
        assert adapter.dataset_id == 1
        assert "catalog_sinan.duckdb" in str(adapter.db_local)


class TestColumnsAdapter:
    def test_init(self, tmp_path):
        from pysus.api.ducklake.catalog.adapters import ColumnsAdapter

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = ColumnsAdapter()
        assert "catalog_columns.duckdb" in str(adapter.db_local)


class TestDownloadCatalogForce:
    @pytest.mark.asyncio
    async def test_download_catalog_force_bypasses_size_check(self, tmp_path):
        from pysus.api.ducklake.catalog.adapters import CatalogAdapter

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = CatalogAdapter()
        local = tmp_path / "test.duckdb"
        local.write_text("existing")

        with patch(
            "pysus.api.ducklake.catalog.adapters.download_http",
            new_callable=AsyncMock,
        ) as mock_dl:
            await adapter._download_catalog(local, "remote/path", force=True)
        mock_dl.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_download_catalog_local_error_fallback(self, tmp_path):
        from pysus.api.ducklake.catalog.adapters import CatalogAdapter

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = CatalogAdapter()
        local = tmp_path / "test.duckdb"
        local.write_text("data")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-length": "4"}
        mock_response.raise_for_status = MagicMock()

        async def fake_head(*args, **kwargs):
            return mock_response

        with patch("httpx.AsyncClient.head", new=fake_head):
            await adapter._download_catalog(local, "remote/path", force=False)


class TestDownloadCatalogErrorCleanup:
    @pytest.mark.asyncio
    async def test_download_fails_cleans_up_local_file(self, tmp_path):
        from pysus.api.ducklake.catalog.adapters import CatalogAdapter

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = CatalogAdapter()
        local = tmp_path / "test.duckdb"
        local.write_text("partial download data")

        with patch(
            "pysus.api.ducklake.catalog.adapters.download_http",
            side_effect=RuntimeError("download failed"),
        ):
            with pytest.raises(RuntimeError, match="download failed"):
                await adapter._download_catalog(
                    local, "remote/path", force=True
                )
        assert not local.exists()

    @pytest.mark.asyncio
    async def test_download_unlink_os_error_swallowed(self, tmp_path):
        from pysus.api.ducklake.catalog.adapters import CatalogAdapter

        with patch("pysus.api.ducklake.catalog.adapters.CACHEPATH", tmp_path):
            adapter = CatalogAdapter()
        local = tmp_path / "test.duckdb"
        local.write_text("partial")

        with patch(
            "pysus.api.ducklake.catalog.adapters.download_http",
            side_effect=RuntimeError("fail"),
        ):
            with patch("pathlib.Path.unlink", side_effect=OSError):
                with pytest.raises(RuntimeError):
                    await adapter._download_catalog(
                        local, "remote/path", force=True
                    )
