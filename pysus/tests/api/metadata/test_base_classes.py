"""Tests for the ``.metadata`` wiring on the base classes."""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import pytest
from pysus.api.metadata.extractors import MetadataExtractor
from pysus.api.metadata.models import (
    DescriptionFacet,
    MetadataBag,
    ProvenanceFacet,
)
from pysus.api.models import (
    BaseRemoteClient,
    BaseRemoteDataset,
    BaseRemoteFile,
    BaseRemoteGroup,
)


class _TitleExtractor(MetadataExtractor):
    """Test extractor that emits a description with the entity name."""

    origin = "test"

    def _extract(self, obj) -> MetadataBag:
        return MetadataBag(
            description=DescriptionFacet(title=obj.name),
            provenance=ProvenanceFacet(origin=self.origin),
        )


class _FailingExtractor(MetadataExtractor):
    """Extractor that always raises — must be ignored."""

    origin = "test"

    def _extract(self, obj) -> MetadataBag:
        raise RuntimeError("boom")


class _TestFile(BaseRemoteFile):
    extractor_types: ClassVar[list] = [_TitleExtractor]

    def __init__(self, name: str = "file.txt"):
        super().__init__(
            path=Path("/tmp") / name,
            type="FILE",
            dataset=_TestDataset(),
        )

    @property
    def extension(self) -> str:
        return ".txt"

    @property
    def size(self) -> int:
        return 10

    @property
    def modify(self):
        from datetime import datetime

        return datetime(2026, 1, 1)

    async def _download(self, output=None, callback=None):
        return output


class _TestGroup(BaseRemoteGroup):
    extractor_types: ClassVar[list] = [_TitleExtractor]

    def __init__(self):
        super().__init__(dataset=_TestDataset())

    @property
    def name(self) -> str:
        return "group-1"

    @property
    def long_name(self) -> str:
        return "Group 1"

    @property
    def description(self) -> str:
        return ""

    async def _fetch_files(self):
        return []


class _TestDataset(BaseRemoteDataset):
    extractor_types: ClassVar[list] = [_TitleExtractor]

    def __init__(self):
        super().__init__(client=_TestClient())

    @property
    def name(self) -> str:
        return "SINAN"

    @property
    def long_name(self) -> str:
        return "Sistema de Informação"

    @property
    def description(self) -> str:
        return ""

    async def _fetch_content(self):
        return []


class _TestClient(BaseRemoteClient):
    extractor_types: ClassVar[list] = [_TitleExtractor]

    @property
    def name(self) -> str:
        return "TestClient"

    @property
    def long_name(self) -> str:
        return "Test Client"

    @property
    def description(self) -> str:
        return ""

    async def connect(self):
        pass

    async def close(self):
        pass

    async def login(self, **kwargs):
        pass

    async def datasets(self, **kwargs):
        return []

    async def download(self, file, output, callback=None):
        return output


class TestFileMetadata:
    def test_returns_merged_bag(self):
        file = _TestFile(name="x.txt")
        bag = file.metadata
        assert isinstance(bag, MetadataBag)
        assert bag.description.title == "x.txt"
        assert bag.provenance.origin == "test"

    def test_result_is_cached(self):
        file = _TestFile(name="x.txt")
        first = file.metadata
        second = file.metadata
        assert first is second


class TestGroupMetadata:
    def test_returns_bag(self):
        group = _TestGroup()
        bag = group.metadata
        assert bag.description.title == "group-1"


class TestDatasetMetadata:
    def test_returns_bag(self):
        dataset = _TestDataset()
        bag = dataset.metadata
        assert bag.description.title == "SINAN"


class TestClientMetadata:
    def test_returns_bag(self):
        client = _TestClient()
        bag = client.metadata
        assert bag.description.title == "TestClient"


class TestFailingExtractor:
    def test_failures_are_ignored(self):
        class _FailingFile(_TestFile):
            extractor_types: ClassVar[list] = [
                _FailingExtractor,
                _TitleExtractor,
            ]

        file = _FailingFile(name="x.txt")
        bag = file.metadata
        # The failing extractor is skipped; the title extractor wins.
        assert bag.description.title == "x.txt"


class TestNoExtractors:
    def test_empty_bag_when_no_extractors(self):
        class _BareFile(BaseRemoteFile):
            def __init__(self):
                super().__init__(
                    path=Path("/tmp") / "x",
                    type="FILE",
                    dataset=_TestDataset(),
                )

            @property
            def extension(self) -> str:
                return ""

            @property
            def size(self) -> int:
                return 0

            @property
            def modify(self):
                from datetime import datetime

                return datetime(2026, 1, 1)

            async def _download(self, output=None, callback=None):
                return output

        file = _BareFile()
        assert file.metadata == MetadataBag()


class TestAsyncMetadata:
    @pytest.mark.asyncio
    async def test_ametadata_delegates_to_extract(self):
        file = _TestFile(name="x.txt")
        bag = await file.ametadata()
        assert bag.description.title == "x.txt"
