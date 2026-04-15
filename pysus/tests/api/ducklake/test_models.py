from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pysus.api.ducklake.models import Dataset, File, Group


@pytest.fixture
def mock_catalog_file():
    from pysus.api.ducklake.catalog import CatalogFile

    file = MagicMock(spec=CatalogFile)
    file.path = Path("test.parquet")
    file.size = 1000
    file.rows = 100
    file.modified = datetime(2026, 1, 1)
    file.sha256 = "abc123"
    return file


@pytest.fixture
def mock_catalog_dataset():
    from pysus.api.ducklake.catalog import CatalogDataset

    dataset = MagicMock(spec=CatalogDataset)
    dataset.name = "test_dataset"
    dataset.long_name = "Test Dataset"
    dataset.description = "Test Description"
    dataset.groups = []
    dataset.files = []
    return dataset


@pytest.fixture
def mock_catalog_group():
    from pysus.api.ducklake.catalog import DatasetGroup

    group = MagicMock(spec=DatasetGroup)
    group.name = "test_group"
    group.long_name = "Test Group"
    group.description = "Test Group Description"
    group.files = []
    return group


@pytest.fixture
def mock_client():
    return MagicMock()


class TestFile:
    @pytest.mark.asyncio
    async def test_file_properties(
        self, mock_catalog_file, mock_catalog_dataset
    ):
        file = File(
            path="test.parquet",
            record=mock_catalog_file,
            parent=mock_catalog_dataset,
            dataset=mock_catalog_dataset,
        )

        assert file.basename == "test.parquet"
        assert file.extension == ".parquet"
        assert file.size == 1000
        assert file.rows == 100
        assert file.sha256 == "abc123"

    @pytest.mark.asyncio
    async def test_file_modify(self, mock_catalog_file, mock_catalog_dataset):
        file = File(
            path="test.parquet",
            record=mock_catalog_file,
            parent=mock_catalog_dataset,
            dataset=mock_catalog_dataset,
        )

        assert file.modify == datetime(2026, 1, 1)

    @pytest.mark.asyncio
    async def test_file_verify_without_sha256(
        self, mock_catalog_file, mock_catalog_dataset
    ):
        mock_catalog_file.sha256 = None
        file = File(
            path="test.parquet",
            record=mock_catalog_file,
            parent=mock_catalog_dataset,
            dataset=mock_catalog_dataset,
        )

        result = await file.verify(Path("/tmp/test.parquet"))
        assert result is True

    @pytest.mark.asyncio
    async def test_file_download(
        self, mock_catalog_file, mock_catalog_dataset, mock_client, tmp_path
    ):
        mock_catalog_file.sha256 = None
        file = File(
            path="test.parquet",
            record=mock_catalog_file,
            parent=mock_catalog_dataset,
            dataset=mock_catalog_dataset,
        )
        file.client = mock_client

        output = tmp_path / "output.parquet"
        await file._download(output=output)

        mock_client._download_file.assert_called_once()


class TestGroup:
    @pytest.mark.asyncio
    async def test_group_properties(
        self, mock_catalog_group, mock_catalog_dataset
    ):
        group = Group(
            record=mock_catalog_group,
            dataset=mock_catalog_dataset,
        )

        assert group.name == "test_group"
        assert group.long_name == "Test Group"
        assert group.description == "Test Group Description"

    @pytest.mark.asyncio
    async def test_group_fetch_files(
        self, mock_catalog_group, mock_catalog_dataset
    ):
        from pysus.api.ducklake.catalog import CatalogFile

        mock_file = MagicMock(spec=CatalogFile)
        mock_file.path = Path("test.parquet")
        mock_catalog_group.files = [mock_file]

        group = Group(
            record=mock_catalog_group,
            dataset=mock_catalog_dataset,
        )

        files = await group._fetch_files()
        assert len(files) == 1
        assert isinstance(files[0], File)


class TestDataset:
    @pytest.mark.asyncio
    async def test_dataset_properties(self, mock_catalog_dataset):
        dataset = Dataset(
            record=mock_catalog_dataset,
            client=MagicMock(),
        )

        assert dataset.name == "test_dataset"
        assert dataset.long_name == "Test Dataset"
        assert dataset.description == "Test Description"

    @pytest.mark.asyncio
    async def test_dataset_fetch_content_with_groups(
        self, mock_catalog_dataset
    ):
        from pysus.api.ducklake.catalog import DatasetGroup

        mock_group = MagicMock(spec=DatasetGroup)
        mock_group.name = "group1"
        mock_catalog_dataset.groups = [mock_group]
        mock_catalog_dataset.files = []

        dataset = Dataset(
            record=mock_catalog_dataset,
            client=MagicMock(),
        )

        content = await dataset._fetch_content()
        assert len(content) == 1
        assert isinstance(content[0], Group)

    @pytest.mark.asyncio
    async def test_dataset_fetch_content_with_files(self, mock_catalog_dataset):
        from pysus.api.ducklake.catalog import CatalogFile

        mock_file = MagicMock(spec=CatalogFile)
        mock_file.path = Path("test.parquet")
        mock_catalog_dataset.groups = []
        mock_catalog_dataset.files = [mock_file]

        dataset = Dataset(
            record=mock_catalog_dataset,
            client=MagicMock(),
        )

        content = await dataset._fetch_content()
        assert len(content) == 1
        assert isinstance(content[0], File)

    @pytest.mark.asyncio
    async def test_dataset_repr(self, mock_catalog_dataset):
        dataset = Dataset(
            record=mock_catalog_dataset,
            client=MagicMock(),
        )

        assert repr(dataset) == "TEST_DATASET"
