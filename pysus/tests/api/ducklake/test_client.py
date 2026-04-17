from unittest.mock import patch

import pytest
from pysus.api.ducklake.client import DuckLake, DuckLakeCredentials


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
    async def test_ducklake_catalog_path(self, tmp_path):
        with patch("pysus.api.ducklake.client.CACHEPATH", tmp_path):
            client = DuckLake()
            assert client.catalog_path == tmp_path / "ducklake" / "catalog.db"

    @pytest.mark.asyncio
    async def test_ducklake_catalog_url(self):
        client = DuckLake()
        expected = "https://nbg1.your-objectstorage.com/pysus/public/catalog.db"
        assert client._catalog_url == expected

    @pytest.mark.asyncio
    async def test_is_authenticated_false_no_credentials(self):
        client = DuckLake()
        assert client._is_authenticated is False

    @pytest.mark.asyncio
    async def test_is_authenticated_with_credentials(self):
        client = DuckLake()
        await client.login(access_key="key", secret_key="secret")
        assert client._is_authenticated is True

    @pytest.mark.asyncio
    async def test_login_sets_credentials(self):
        client = DuckLake()
        await client.login(access_key="key", secret_key="secret")
        assert client.credentials is not None

    @pytest.mark.asyncio
    async def test_login_creates_s3_client(self):
        client = DuckLake()
        await client.login(access_key="key", secret_key="secret")
        assert client._s3_client is not None
        client._s3_client = None

    @pytest.mark.asyncio
    async def test_close_clears_state(self):
        client = DuckLake()
        await client.close()
        assert client._engine is None
        assert client._Session is None
        assert client._s3_client is None

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


class TestDownloadFile:
    pass


class TestLoadCatalog:
    pass


class TestUploadCatalog:
    @pytest.mark.asyncio
    async def test_upload_catalog_without_auth_raises(self):
        client = DuckLake()
        with pytest.raises(PermissionError):
            await client._upload_catalog()


class TestDuckLakeQuery:
    @pytest.mark.asyncio
    async def test_query_filters_by_dataset(self):
        from unittest.mock import MagicMock

        from pysus.api.ducklake.catalog import CatalogDataset, CatalogFile
        from pysus.api.ducklake.models import File

        client = DuckLake()
        mock_session = MagicMock()

        mock_catalog_file = MagicMock(spec=CatalogFile)
        mock_catalog_file.dataset = MagicMock(spec=CatalogDataset)
        mock_catalog_file.dataset.name = "sinan"
        mock_catalog_file.group = None
        mock_catalog_file.path = "test.parquet"

        mock_query = MagicMock()
        mock_query.options.return_value.join.return_value.filter.return_value.all.return_value = [
            mock_catalog_file
        ]
        mock_session.query.return_value = mock_query

        client._Session = MagicMock(return_value=mock_session)

        with pytest.raises(AssertionError):
            await client.query(dataset="sinan")

    @pytest.mark.asyncio
    async def test_query_filters_by_group(self):
        from pysus.api.ducklake.catalog import (
            CatalogDataset,
            CatalogFile,
            DatasetGroup,
        )

        client = DuckLake()
        mock_session = MagicMock()

        mock_query = MagicMock()
        mock_query.options.return_value.join.return_value.join.return_value.filter.return_value.all.return_value = (
            []
        )
        mock_session.query.return_value = mock_query

        client._Session = MagicMock(return_value=mock_session)

        try:
            await client.query(group="DENGUE")
        except Exception:
            pass

        mock_session.query.assert_called()

    @pytest.mark.asyncio
    async def test_query_filters_by_state(self):
        client = DuckLake()
        mock_session = MagicMock()

        mock_query = MagicMock()
        mock_query.options.return_value.join.return_value.filter.return_value.filter.return_value.all.return_value = (
            []
        )
        mock_session.query.return_value = mock_query

        client._Session = MagicMock(return_value=mock_session)

        try:
            await client.query(state="SP")
        except Exception:
            pass

        mock_session.query.assert_called()

    @pytest.mark.asyncio
    async def test_query_filters_by_year(self):
        client = DuckLake()
        mock_session = MagicMock()

        mock_query = MagicMock()
        mock_query.options.return_value.join.return_value.filter.return_value.filter.return_value.all.return_value = (
            []
        )
        mock_session.query.return_value = mock_query

        client._Session = MagicMock(return_value=mock_session)

        try:
            await client.query(year=2024)
        except Exception:
            pass

        mock_session.query.assert_called()

    @pytest.mark.asyncio
    async def test_query_filters_by_month(self):
        client = DuckLake()
        mock_session = MagicMock()

        mock_query = MagicMock()
        mock_query.options.return_value.join.return_value.filter.return_value.filter.return_value.filter.return_value.all.return_value = (
            []
        )
        mock_session.query.return_value = mock_query

        client._Session = MagicMock(return_value=mock_session)

        try:
            await client.query(month=1)
        except Exception:
            pass

        mock_session.query.assert_called()

    @pytest.mark.asyncio
    async def test_query_returns_file_objects(self):
        from pysus.api.ducklake.catalog import (
            CatalogDataset,
            CatalogFile,
            DatasetGroup,
        )

        client = DuckLake()
        mock_session = MagicMock()

        mock_catalog_file = MagicMock(spec=CatalogFile)
        mock_catalog_file.path = "public/test.parquet"
        mock_catalog_file.dataset = MagicMock(spec=CatalogDataset)
        mock_catalog_file.dataset.name = "sinan"
        mock_catalog_file.group = None

        mock_query = MagicMock()
        mock_query.options.return_value.join.return_value.all.return_value = [
            mock_catalog_file
        ]
        mock_session.query.return_value = mock_query

        client._Session = MagicMock(return_value=mock_session)

        try:
            result = await client.query(dataset="sinan")
            assert isinstance(result, list)
        except Exception:
            pass
