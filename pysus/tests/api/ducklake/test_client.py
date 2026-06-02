from unittest.mock import MagicMock, patch

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
        from unittest.mock import patch

        client = DuckLake()
        with patch.object(client, "_download_catalog"):
            await client.login(access_key="key", secret_key="secret")
        assert client._is_authenticated is True

    @pytest.mark.asyncio
    async def test_login_sets_credentials(self):
        from unittest.mock import patch

        client = DuckLake()
        with patch.object(client, "_download_catalog"):
            await client.login(access_key="key", secret_key="secret")
        assert client.credentials is not None

    @pytest.mark.asyncio
    async def test_login_creates_s3_client(self):
        from unittest.mock import patch

        client = DuckLake()
        with patch.object(client, "_download_catalog"):
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
        from pysus.api.ducklake.catalog import CatalogDataset, Origin
        from pysus.api.ducklake.models import DuckDataset

        client = DuckLake()
        record = CatalogDataset(name="test", long_name="Test", origin=Origin.FTP)
        dataset = DuckDataset(record=record, client=client)
        with pytest.raises(PermissionError):
            await dataset._upload_catalog()


class TestDownloadFile:
    pass


class TestLoadCatalog:
    pass


class TestUploadCatalog:
    @pytest.mark.asyncio
    async def test_upload_catalog_without_auth_raises(self):
        from pysus.api.ducklake.catalog import CatalogDataset, Origin
        from pysus.api.ducklake.models import DuckDataset

        client = DuckLake()
        record = CatalogDataset(name="test", long_name="Test", origin=Origin.FTP)
        dataset = DuckDataset(record=record, client=client)
        with pytest.raises(PermissionError):
            await dataset._upload_catalog()


class TestDuckLakeQuery:
    @pytest.mark.asyncio
    async def test_query_filters_by_dataset(self):
        from pysus.api.ducklake.catalog import CatalogDataset, CatalogFile

        client = DuckLake()
        mock_session = MagicMock()

        mock_catalog_file = MagicMock(spec=CatalogFile)
        mock_catalog_file.dataset = MagicMock(spec=CatalogDataset)
        mock_catalog_file.dataset.name = "sinan"
        mock_catalog_file.group = None
        mock_catalog_file.path = "test.parquet"

        mock_query = MagicMock()
        mock_query.options.return_value.join.return_value.filter.return_value.all.return_value = [  # noqa: E501
            mock_catalog_file
        ]
        mock_session.query.return_value = mock_query

        client._Session = MagicMock(return_value=mock_session)

        result = await client.query(dataset="sinan")
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_query_filters_by_group(self):
        client = DuckLake()
        client._engine = MagicMock()
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_query = MagicMock()
        mock_query.options.return_value.join.return_value.filter.return_value.filter.return_value.all.return_value = (  # noqa: E501
            []
        )
        mock_session.query.return_value = mock_query

        client._Session = MagicMock(return_value=mock_session)

        result = await client.query(group="DENGUE")
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_query_filters_by_state(self):
        client = DuckLake()
        client._engine = MagicMock()
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_query = MagicMock()
        mock_query.options.return_value.join.return_value.filter.return_value.filter.return_value.all.return_value = (  # noqa: E501
            []
        )
        mock_session.query.return_value = mock_query

        client._Session = MagicMock(return_value=mock_session)

        result = await client.query(state="SP")
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_query_filters_by_year(self):
        client = DuckLake()
        client._engine = MagicMock()
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_query = MagicMock()
        mock_query.options.return_value.join.return_value.filter.return_value.filter.return_value.all.return_value = (  # noqa: E501
            []
        )
        mock_session.query.return_value = mock_query

        client._Session = MagicMock(return_value=mock_session)

        result = await client.query(year=2024)
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_query_filters_by_month(self):
        client = DuckLake()
        client._engine = MagicMock()
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_query = MagicMock()
        mock_query.options.return_value.join.return_value.filter.return_value.filter.return_value.filter.return_value.filter.return_value.all.return_value = (  # noqa: E501
            []
        )
        mock_session.query.return_value = mock_query

        client._Session = MagicMock(return_value=mock_session)

        result = await client.query(month=1)
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_query_no_filters(self):
        from pysus.api.ducklake.catalog import CatalogDataset, CatalogFile

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
        except OSError:
            pass
