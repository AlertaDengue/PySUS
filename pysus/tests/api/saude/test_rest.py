"""Tests for pysus.api.saude.rest — DEMAS REST helpers."""

from __future__ import annotations

import json
import pathlib
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from pysus.api.saude.rest import (
    EndpointSpec,
    endpoints_from_swagger,
    fetch_swagger,
    iter_rows,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


# -- EndpointSpec ---------------------------------------------------------


class TestEndpointSpec:
    def test_defaults(self):
        spec = EndpointSpec(path="/foo/bar")
        assert spec.path == "/foo/bar"
        assert spec.summary == ""
        assert spec.params == ()
        assert spec.tag == ""
        assert spec.limit == 1000

    def test_custom(self):
        spec = EndpointSpec(
            path="/test",
            summary="Test",
            params=("a", "b"),
            tag="TestTag",
            limit=500,
        )
        assert spec.params == ("a", "b")
        assert spec.limit == 500

    def test_frozen(self):
        spec = EndpointSpec(path="/x")
        with pytest.raises(AttributeError):
            spec.path = "/y"  # type: ignore[misc]


# -- endpoints_from_swagger -----------------------------------------------


@pytest.fixture
def swagger():
    return json.loads(
        (FIXTURES / "demas_swagger.json").read_text(encoding="utf-8")
    )


class TestEndpointsFromSwagger:
    def test_all_endpoints(self, swagger):
        specs = endpoints_from_swagger(swagger)
        assert len(specs) == 4

    def test_filter_by_tag(self, swagger):
        specs = endpoints_from_swagger(swagger, tag="Agravo Arboviroses")
        assert len(specs) == 2
        assert all(s.tag == "Agravo Arboviroses" for s in specs)
        paths = {s.path for s in specs}
        assert "/arboviroses/dengue" in paths
        assert "/arboviroses/chikungunya" in paths

    def test_cnes_tag(self, swagger):
        specs = endpoints_from_swagger(swagger, tag="CNES")
        assert len(specs) == 1
        assert specs[0].path == "/estabelecimentos"
        assert "codigo_uf" in specs[0].params

    def test_no_match(self, swagger):
        specs = endpoints_from_swagger(swagger, tag="NonExistent")
        assert specs == []

    def test_params_extracted(self, swagger):
        specs = endpoints_from_swagger(swagger, tag="CNES")
        assert "codigo_uf" in specs[0].params
        assert "limit" in specs[0].params
        assert "offset" in specs[0].params

    def test_path_without_get_skipped(self):
        swagger = {
            "paths": {
                "/post-only": {"post": {"summary": "Create"}},
                "/with-get": {
                    "get": {"summary": "List", "tags": ["T"], "parameters": []}
                },
            }
        }
        specs = endpoints_from_swagger(swagger)
        assert len(specs) == 1
        assert specs[0].path == "/with-get"


# -- _extract_rows --------------------------------------------------------


def test_extract_rows_list():
    from pysus.api.saude.rest import _extract_rows

    assert _extract_rows([{"a": 1}, {"a": 2}]) == [{"a": 1}, {"a": 2}]


def test_extract_rows_envelope():
    from pysus.api.saude.rest import _extract_rows

    assert _extract_rows({"dengue": [{"x": 1}]}) == [{"x": 1}]


def test_extract_rows_empty_dict():
    from pysus.api.saude.rest import _extract_rows

    assert _extract_rows({"not_a_list": "foo"}) == []


def test_extract_rows_non_list():
    from pysus.api.saude.rest import _extract_rows

    assert _extract_rows("string") == []


# -- iter_rows ------------------------------------------------------------


@pytest.mark.asyncio
async def test_iter_rows_basic():
    """iter_rows should yield rows from paginated responses."""
    rows_page0 = [{"id": i} for i in range(1000)]
    rows_page1 = [{"id": i} for i in range(1000, 1500)]

    def _resp(data):
        return httpx.Response(
            200,
            json=data,
            request=httpx.Request("GET", "https://test/"),
        )

    responses = [_resp({"items": rows_page0}), _resp({"items": rows_page1})]

    async def mock_get(url, params=None):
        return responses.pop(0)

    client = MagicMock()
    client.get = mock_get

    collected = []
    async for row in iter_rows(client, "/test", page_size=1000):
        collected.append(row)

    assert len(collected) == 1500
    assert collected[0] == {"id": 0}
    assert collected[1499] == {"id": 1499}


@pytest.mark.asyncio
async def test_iter_rows_empty():
    """Empty response should yield nothing."""
    client = MagicMock()
    client.get = AsyncMock(
        return_value=httpx.Response(
            200,
            json={"items": []},
            request=httpx.Request("GET", "https://test/"),
        )
    )

    collected = []
    async for row in iter_rows(client, "/empty"):
        collected.append(row)
    assert collected == []


@pytest.mark.asyncio
async def test_iter_rows_limit():
    """The limit parameter should cap total rows."""
    rows = [{"id": i} for i in range(500)]
    client = MagicMock()
    client.get = AsyncMock(
        return_value=httpx.Response(
            200,
            json={"data": rows},
            request=httpx.Request("GET", "https://test/"),
        )
    )

    collected = []
    async for row in iter_rows(client, "/limited", limit=100):
        collected.append(row)
    assert len(collected) == 100


@pytest.mark.asyncio
async def test_iter_rows_with_params():
    """Extra params should be passed to the request."""
    called_params = []

    async def mock_get(url, params=None):
        called_params.append(params)
        return httpx.Response(
            200,
            json={"items": []},
            request=httpx.Request("GET", "https://test/"),
        )

    client = MagicMock()
    client.get = mock_get

    async for _ in iter_rows(client, "/filtered", params={"nu_ano": "2024"}):
        pass

    assert called_params[0]["nu_ano"] == "2024"
    assert called_params[0]["limit"] == 1000
    assert called_params[0]["offset"] == 0


@pytest.mark.asyncio
async def test_iter_rows_offset_advances():
    """Offset should advance by len(rows) each page."""
    offsets = []

    async def mock_get(url, params=None):
        offsets.append(params["offset"])
        if params["offset"] == 0:
            return httpx.Response(
                200,
                json={"items": [{"id": 0}, {"id": 1}]},
                request=httpx.Request("GET", "https://test/"),
            )
        return httpx.Response(
            200,
            json={"items": []},
            request=httpx.Request("GET", "https://test/"),
        )

    client = MagicMock()
    client.get = mock_get

    async for _ in iter_rows(client, "/test", page_size=2):
        pass

    assert offsets == [0, 2]


@pytest.mark.asyncio
async def test_iter_rows_bare_list_response():
    """Handle responses that return a bare list (no envelope)."""
    client = MagicMock()
    client.get = AsyncMock(
        return_value=httpx.Response(
            200,
            json=[{"a": 1}, {"a": 2}],
            request=httpx.Request("GET", "https://test/"),
        )
    )

    collected = []
    async for row in iter_rows(client, "/bare"):
        collected.append(row)
    assert len(collected) == 2


# -- fetch_swagger --------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_swagger_caches(tmp_path):
    """fetch_swagger should write the swagger JSON to the cache dir."""
    swagger_data = {
        "openapi": "2.0",
        "info": {"version": "5.32.12"},
        "paths": {},
    }
    client = MagicMock()
    client.get = AsyncMock(
        return_value=httpx.Response(
            200,
            json=swagger_data,
            request=httpx.Request("GET", "https://test/"),
        )
    )

    result = await fetch_swagger(client, tmp_path, ttl=timedelta(hours=1))
    assert result["info"]["version"] == "5.32.12"
    cached = tmp_path / "demas_swagger.json"
    assert cached.exists()
    assert json.loads(cached.read_text()) == swagger_data


@pytest.mark.asyncio
async def test_fetch_swagger_uses_cache(tmp_path):
    """If the cache is fresh, fetch_swagger should not make a request."""
    swagger_data = {"cached": True}
    cache_file = tmp_path / "demas_swagger.json"
    cache_file.write_text(json.dumps(swagger_data))

    client = MagicMock()
    client.get = AsyncMock()  # should not be called

    result = await fetch_swagger(
        client, tmp_path, ttl=timedelta(hours=1), use_cache=True
    )
    assert result == swagger_data
    client.get.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_swagger_force_refresh(tmp_path):
    """use_cache=False should force a fresh download."""
    old = {"version": "old"}
    cache_file = tmp_path / "demas_swagger.json"
    cache_file.write_text(json.dumps(old))

    new = {"version": "new"}
    client = MagicMock()
    client.get = AsyncMock(
        return_value=httpx.Response(
            200,
            json=new,
            request=httpx.Request("GET", "https://test/"),
        )
    )

    result = await fetch_swagger(
        client, tmp_path, ttl=timedelta(hours=1), use_cache=False
    )
    assert result == {"version": "new"}
    client.get.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_swagger_corrupt_cache_refetches(tmp_path):
    """Corrupt cache file should trigger a fresh download."""
    cache_file = tmp_path / "demas_swagger.json"
    cache_file.write_text("not valid json {{{")

    new = {"version": "fresh"}
    client = MagicMock()
    client.get = AsyncMock(
        return_value=httpx.Response(
            200,
            json=new,
            request=httpx.Request("GET", "https://test/"),
        )
    )

    result = await fetch_swagger(
        client, tmp_path, ttl=timedelta(hours=1), use_cache=True
    )
    assert result == new
    client.get.assert_called_once()
