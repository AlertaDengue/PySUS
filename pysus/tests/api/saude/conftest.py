"""pytest fixtures for the Saude client.

Uses ``httpx.MockTransport`` to serve the captured payloads so the test
suite runs entirely offline.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

import httpx
import pytest

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


def _load(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _build_mock_transport() -> httpx.MockTransport:
    """Return a transport that serves the captured fixtures."""

    homepage = _load("homepage.html")
    catalog_page1 = _load("catalog_page1.json")
    dataset_dengue = _load("dataset_arboviroses-dengue.json")
    resource_zip = _load("dengue_2024.csv.zip")

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        path = request.url.path
        if path in ("/", "") or url.endswith("dadosabertos.saude.gov.br"):
            return httpx.Response(200, content=homepage)
        if path.endswith("/dataset.json"):
            return httpx.Response(200, content=catalog_page1)
        if path.endswith("/dataset/arboviroses-dengue.json"):
            return httpx.Response(200, content=dataset_dengue)
        if "ckan.saude.gov.br" in url:
            return httpx.Response(200, content=resource_zip)
        return httpx.Response(404, content=b"not found: " + url.encode())

    return httpx.MockTransport(handler)


@pytest.fixture
def mocked_saude() -> httpx.MockTransport:
    """A mock transport that serves the captured fixtures."""
    return _build_mock_transport()


@pytest.fixture
def saude_client(tmp_path: pathlib.Path) -> Any:
    """A ready-to-use SaudeClient wired to the mock transport."""
    import asyncio

    from pysus.api.saude import SaudeClient

    client = SaudeClient(cache_dir=tmp_path, timeout=10.0)
    client._client = httpx.AsyncClient(
        transport=_build_mock_transport(), timeout=10.0
    )
    yield client
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(client.close())
    else:
        loop.create_task(client.close())


@pytest.fixture
def saude_homepage_html() -> str:
    """Raw homepage HTML for buildId-extraction tests."""
    return _load("homepage.html").decode("utf-8")


@pytest.fixture
def saude_catalog_payload() -> dict[str, Any]:
    """Parsed ``dataset.json?page=1`` payload."""
    return json.loads(_load("catalog_page1.json"))


@pytest.fixture
def saude_dataset_payload() -> dict[str, Any]:
    """Parsed ``dataset/arboviroses-dengue.json`` payload."""
    return json.loads(_load("dataset_arboviroses-dengue.json"))


@pytest.fixture
def saude_dataset_page_props(
    saude_dataset_payload: dict[str, Any]
) -> dict[str, Any]:
    """Just the ``pageProps`` of the dengue dataset payload."""
    return saude_dataset_payload["pageProps"]


@pytest.fixture
def saude_resource_zip_path(tmp_path: pathlib.Path) -> pathlib.Path:
    """Path to the (synthetic) dengue CSV.ZIP fixture."""
    target = tmp_path / "dengue_2024.csv.zip"
    target.write_bytes(_load("dengue_2024.csv.zip"))
    return target
