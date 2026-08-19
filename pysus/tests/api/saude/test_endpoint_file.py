"""Tests for SaudeEndpointFile model and SaudeEndpointFileExtractor."""

from __future__ import annotations

import json
import pathlib
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from pysus.api.saude.metadata import SaudeEndpointFileExtractor
from pysus.api.saude.models import SaudeEndpointFile
from pysus.api.saude.rest import EndpointSpec

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


def _make_dataset():
    """Return a minimal BaseRemoteDataset-compatible mock."""
    from pysus.api.saude.databases import DatasetSpec
    from pysus.api.saude.models import SaudeDataset

    spec = DatasetSpec(
        name="TEST",
        long_name="Test",
        description="Test dataset",
        ckan_group=None,
        slug_patterns=(),
        exclude_patterns=(),
        demas_tags=(),
        endpoints=(),
    )
    client = MagicMock()
    client._client = MagicMock()
    return SaudeDataset.model_construct(spec=spec, client=client)


# -- SaudeEndpointFile ---------------------------------------------------


class TestSaudeEndpointFile:
    def _make_file(
        self, path: str = "/arboviroses/dengue"
    ) -> SaudeEndpointFile:
        spec = EndpointSpec(
            path=path, summary="Dengue", tag="Agravo Arboviroses"
        )
        return SaudeEndpointFile(
            record=spec,
            dataset=_make_dataset(),
            path=pathlib.Path(f"/tmp/test_{path.strip('/').replace('/', '_')}"),
        )

    def test_extension(self):
        f = self._make_file()
        assert f.extension == ".jsonl"

    def test_size_zero(self):
        f = self._make_file()
        assert f.size == 0

    def test_modify_raises(self):
        f = self._make_file()
        with pytest.raises(ValueError, match="no modification date"):
            _ = f.modify

    def test_year_none(self):
        f = self._make_file("/arboviroses/dengue")
        assert f.year is None

    def test_year_from_path(self):
        f = self._make_file("/arboviroses/dengue/2024")
        assert f.year is None or isinstance(f.year, int)

    def test_month_none(self):
        f = self._make_file()
        assert f.month is None

    def test_state_none(self):
        f = self._make_file()
        assert f.state is None

    @pytest.mark.asyncio
    async def test_download_writes_jsonl(self, tmp_path):
        """_download should write rows to a JSONL file."""
        rows_page0 = [{"dt_notific": "2024-01-01", "id": i} for i in range(2)]
        rows_page1 = [
            {"dt_notific": "2024-01-02", "id": i} for i in range(2, 3)
        ]

        def _resp(data):
            return httpx.Response(
                200,
                json=data,
                request=httpx.Request("GET", "https://test/"),
            )

        responses = [
            _resp({"dengue": rows_page0}),
            _resp({"dengue": rows_page1}),
        ]

        async def mock_get(url, params=None):
            return responses.pop(0)

        mock_client = MagicMock()
        mock_client.get = mock_get

        ds = _make_dataset()
        ds.client._client = mock_client

        spec = EndpointSpec(path="/arboviroses/dengue", limit=2)
        f = SaudeEndpointFile(
            record=spec,
            dataset=ds,
            path=tmp_path / "dengue",
        )

        output = tmp_path / "dengue.jsonl"
        result = await f._download(output=output)
        assert result == output
        assert output.exists()

        lines = output.read_text().strip().split("\n")
        assert len(lines) == 3
        first = json.loads(lines[0])
        assert first["dt_notific"] == "2024-01-01"
        last = json.loads(lines[2])
        assert last["dt_notific"] == "2024-01-02"

    @pytest.mark.asyncio
    async def test_download_with_callback(self, tmp_path):
        """_download should call the callback after each page."""
        rows = [{"i": 0}]
        client = MagicMock()
        client.get = AsyncMock(
            return_value=httpx.Response(
                200,
                json={"data": rows},
                request=httpx.Request("GET", "https://test/"),
            )
        )
        ds = _make_dataset()
        ds.client._client = client

        f = SaudeEndpointFile(
            record=EndpointSpec(path="/test"),
            dataset=ds,
            path=tmp_path / "test",
        )

        calls = []
        output = tmp_path / "test.jsonl"
        await f._download(
            output=output, callback=lambda n, t: calls.append((n, t))
        )
        assert calls == [(1, 0)]

    @pytest.mark.asyncio
    async def test_fetch_size_returns_zero(self):
        f = self._make_file()
        assert await f.fetch_size() == 0


# -- SaudeEndpointFileExtractor -------------------------------------------


class TestSaudeEndpointFileExtractor:
    def test_identity(self):
        spec = EndpointSpec(
            path="/arboviroses/dengue",
            summary="Dengue data",
            tag="Agravo Arboviroses",
        )
        f = SaudeEndpointFile(
            record=spec,
            dataset=_make_dataset(),
            path=pathlib.Path("/tmp/test_dengue"),
        )
        ext = SaudeEndpointFileExtractor()
        bag = ext.extract(f)
        assert bag.identity.name == "/arboviroses/dengue"

    def test_access(self):
        spec = EndpointSpec(
            path="/arboviroses/dengue",
            summary="Dengue data",
            tag="Agravo Arboviroses",
        )
        f = SaudeEndpointFile(
            record=spec,
            dataset=_make_dataset(),
            path=pathlib.Path("/tmp/test_dengue"),
        )
        ext = SaudeEndpointFileExtractor()
        bag = ext.extract(f)
        assert (
            bag.access.url
            == "https://apidadosabertos.saude.gov.br/arboviroses/dengue"
        )
        assert bag.access.format == "jsonl"
        assert bag.access.download_strategy == "http-paged"

    def test_structure(self):
        spec = EndpointSpec(path="/test")
        f = SaudeEndpointFile(
            record=spec,
            dataset=_make_dataset(),
            path=pathlib.Path("/tmp/test"),
        )
        ext = SaudeEndpointFileExtractor()
        bag = ext.extract(f)
        assert bag.structure.format == "jsonl"

    def test_provenance(self):
        spec = EndpointSpec(path="/test")
        f = SaudeEndpointFile(
            record=spec,
            dataset=_make_dataset(),
            path=pathlib.Path("/tmp/test"),
        )
        ext = SaudeEndpointFileExtractor()
        bag = ext.extract(f)
        assert bag.provenance.origin == "saude"

    def test_supported_facets(self):
        ext = SaudeEndpointFileExtractor()
        assert ext.supported_facets() == {
            "identity",
            "description",
            "structure",
            "access",
            "provenance",
        }

    def test_description_from_summary(self):
        spec = EndpointSpec(
            path="/test",
            summary="Test endpoint summary",
        )
        f = SaudeEndpointFile(
            record=spec,
            dataset=_make_dataset(),
            path=pathlib.Path("/tmp/test"),
        )
        ext = SaudeEndpointFileExtractor()
        bag = ext.extract(f)
        assert bag.description.title == "Test endpoint summary"
        assert bag.description.long_name == "Test endpoint summary"
