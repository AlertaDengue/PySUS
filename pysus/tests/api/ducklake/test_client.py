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
