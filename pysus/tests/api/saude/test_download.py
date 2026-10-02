"""Tests for the resource / dataset download helpers."""

from __future__ import annotations

import httpx
import pytest
from pysus.api.saude.download import (
    _select_resource,
    download_dataset,
    download_resource,
    filename_for,
)
from pysus.api.saude.errors import ResourceNotFound
from pysus.api.saude.resources import CKANPackage, Resource


class TestFilenameFor:
    def test_uses_resource_name(self):
        resource = Resource.model_validate(
            {
                "id": "abc",
                "name": "Dengue - 2024",
                "format": "CSV",
                "url": "https://example.com/x.csv",
            }
        )
        assert (
            filename_for(resource, "arboviroses-dengue", 0)
            == "Dengue - 2024.csv"
        )

    def test_appends_format_extension_when_missing(self):
        resource = Resource.model_validate(
            {
                "id": "abc",
                "name": "Dengue 2024",
                "format": "JSON",
                "url": "https://example.com/x",
            }
        )
        assert (
            filename_for(resource, "arboviroses-dengue", 0)
            == "Dengue 2024.json"
        )

    def test_skips_extension_when_format_is_api(self):
        resource = Resource.model_validate(
            {
                "id": "abc",
                "name": "API docs",
                "format": "API",
                "url": "https://example.com/docs",
            }
        )
        assert filename_for(resource, "x", 0) == "API docs"

    def test_strips_unsafe_characters(self):
        resource = Resource.model_validate(
            {
                "id": "abc",
                "name": "weird/name?with&chars",
                "format": "CSV",
                "url": "https://example.com/x",
            }
        )
        name = filename_for(resource, "x", 0)
        assert "/" not in name
        assert "?" not in name
        assert "&" not in name


class TestDownloadResource:
    @pytest.mark.asyncio
    async def test_downloads_single_resource(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        target = next(r for r in package.resources if r.format == "CSV")

        async with httpx.AsyncClient(transport=mocked_saude) as client:
            path = await download_resource(
                client, package, resource_id=target.id, dest_dir=tmp_path
            )
        assert path.exists()
        assert path.stat().st_size > 0

    @pytest.mark.asyncio
    async def test_skips_api_resources(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        api_resource = next(r for r in package.resources if r.format == "API")
        async with httpx.AsyncClient(transport=mocked_saude) as client:
            with pytest.raises(ResourceNotFound):
                await download_resource(
                    client,
                    package,
                    resource_id=api_resource.id,
                    dest_dir=tmp_path,
                )

    @pytest.mark.asyncio
    async def test_ambiguous_selector_raises(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        async with httpx.AsyncClient(transport=mocked_saude) as client:
            with pytest.raises(ValueError, match="exactly one"):
                await download_resource(client, package, dest_dir=tmp_path)

    @pytest.mark.asyncio
    async def test_no_match_raises_not_found(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        async with httpx.AsyncClient(transport=mocked_saude) as client:
            with pytest.raises(ResourceNotFound):
                await download_resource(
                    client,
                    package,
                    resource_id="nonexistent-id",
                    dest_dir=tmp_path,
                )


class TestDownloadDataset:
    @pytest.mark.asyncio
    async def test_downloads_only_csv_resources(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        expected_csvs = sum(1 for r in package.resources if r.format == "CSV")
        assert expected_csvs > 0

        async with httpx.AsyncClient(transport=mocked_saude) as client:
            paths = await download_dataset(
                client,
                package,
                dest_dir=tmp_path,
                fmt="CSV",
            )
        assert len(paths) == expected_csvs
        for path in paths:
            assert path.exists()
            assert path.stat().st_size > 0


class TestSelectResource:
    def _make_package(self):
        resources = [
            Resource(
                id="csv-1",
                name="Dengue CSV",
                format="CSV",
                url="https://example.com/dengue.csv",
            ),
            Resource(
                id="api-1",
                name="Dengue API",
                format="API",
                url="https://example.com/docs",
            ),
            Resource(
                id="json-1",
                name="Dengue JSON",
                format="JSON",
                url="https://example.com/dengue.json",
            ),
        ]
        return CKANPackage(
            id="pkg-1",
            name="arboviroses-dengue",
            title="Arboviroses",
            resources=resources,
            num_resources=len(resources),
            metadata_created="2024-01-01T00:00:00",
            metadata_modified="2024-01-01T00:00:00",
        )

    def test_select_by_id(self):
        pkg = self._make_package()
        r = _select_resource(pkg, resource_id="json-1", name=None, fmt=None)
        assert r.id == "json-1"

    def test_select_by_name(self):
        pkg = self._make_package()
        r = _select_resource(pkg, resource_id=None, name="Dengue CSV", fmt=None)
        assert r.format == "CSV"

    def test_select_by_fmt(self):
        pkg = self._make_package()
        r = _select_resource(
            pkg, resource_id=None, name="Dengue JSON", fmt="JSON"
        )
        assert r.format == "JSON"

    def test_name_mismatch_raises(self):
        pkg = self._make_package()
        with pytest.raises(ResourceNotFound):
            _select_resource(
                pkg, resource_id=None, name="Nonexistent", fmt=None
            )

    def test_fmt_mismatch_raises(self):
        pkg = self._make_package()
        with pytest.raises(ResourceNotFound):
            _select_resource(
                pkg, resource_id=None, name="Dengue CSV", fmt="PDF"
            )

    def test_fmt_filters_out_non_matching(self):
        pkg = self._make_package()
        with pytest.raises(ResourceNotFound):
            _select_resource(
                pkg, resource_id=None, name="Dengue CSV", fmt="XML"
            )


class TestDownloadResourceEdgeCases:
    @pytest.mark.asyncio
    async def test_existing_file_not_overwritten(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        csv_resource = next(r for r in package.resources if r.format == "CSV")
        async with httpx.AsyncClient(transport=mocked_saude) as client:
            path1 = await download_resource(
                client,
                package,
                resource_id=csv_resource.id,
                dest_dir=tmp_path,
            )
            path2 = await download_resource(
                client,
                package,
                resource_id=csv_resource.id,
                dest_dir=tmp_path,
            )
        assert path1 == path2

    @pytest.mark.asyncio
    async def test_progress_callback_called(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        csv_resource = next(r for r in package.resources if r.format == "CSV")
        progress_calls = []

        def progress(downloaded: int, total: int):
            progress_calls.append((downloaded, total))

        async with httpx.AsyncClient(transport=mocked_saude) as client:
            await download_resource(
                client,
                package,
                resource_id=csv_resource.id,
                dest_dir=tmp_path,
                progress=progress,
            )
        assert len(progress_calls) > 0

    @pytest.mark.asyncio
    async def test_no_dest_dir_uses_slug_dir(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        csv_resource = next(r for r in package.resources if r.format == "CSV")
        original_dir = tmp_path / "cwd"
        original_dir.mkdir()

        import os

        old_cwd = os.getcwd()
        try:
            os.chdir(original_dir)
            async with httpx.AsyncClient(transport=mocked_saude) as client:
                path = await download_resource(
                    client,
                    package,
                    resource_id=csv_resource.id,
                )
            assert path.exists()
        finally:
            os.chdir(old_cwd)

    @pytest.mark.asyncio
    async def test_select_by_name(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        csv_resource = next(r for r in package.resources if r.format == "CSV")
        async with httpx.AsyncClient(transport=mocked_saude) as client:
            path = await download_resource(
                client,
                package,
                name=csv_resource.name,
                dest_dir=tmp_path,
            )
        assert path.exists()

    @pytest.mark.asyncio
    async def test_select_by_fmt(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        csv_resource = next(r for r in package.resources if r.format == "CSV")
        async with httpx.AsyncClient(transport=mocked_saude) as client:
            path = await download_resource(
                client,
                package,
                name=csv_resource.name,
                fmt="CSV",
                dest_dir=tmp_path,
            )
        assert path.exists()


class _FlakyTransport(httpx.AsyncBaseTransport):
    """Yields ``good_bytes`` and then fails, once, like a dropped socket.

    The failure happens *after* real bytes have been handed to the caller,
    which is what leaves a truncated file behind when the destination is
    written in place.
    """

    def __init__(self, head: bytes, tail: bytes, fail_once: bool = True):
        self.head = head
        self.tail = tail
        self.fail_once = fail_once
        self.calls = 0

    async def handle_async_request(self, request: httpx.Request):
        self.calls += 1
        should_fail = self.fail_once and self.calls == 1
        return httpx.Response(
            200,
            content=_flaky_stream(
                self.head, self.tail, fail=should_fail, request=request
            ),
            headers={"Content-Length": str(len(self.head) + len(self.tail))},
        )


async def _flaky_stream(
    head: bytes, tail: bytes, fail: bool, request: httpx.Request
):
    yield head
    if fail:
        raise httpx.ReadError("connection reset", request=request)
    yield tail


class TestDownloadResourcePartialTransfer:
    """A dropped connection must not leave a file the retry can reuse.

    ``download_resource`` short-circuits on ``dest_path.exists()``. If a
    failed transfer left a truncated file there, the next attempt would
    return it as a complete download and the caller would convert and
    upload the partial bytes.
    """

    @pytest.mark.asyncio
    async def test_failure_mid_stream_leaves_no_file(
        self, saude_dataset_page_props, tmp_path
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        csv_resource = next(r for r in package.resources if r.format == "CSV")

        head, tail = b"col_a,col_b\n1,2\n", b"3,4\n"
        transport = _FlakyTransport(head, tail)

        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(httpx.ReadError):
                await download_resource(
                    client,
                    package,
                    resource_id=csv_resource.id,
                    dest_dir=tmp_path,
                )

        # Nothing at the derived name, and no leftover .part scratch file.
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.asyncio
    async def test_retry_after_failure_downloads_full_content(
        self, saude_dataset_page_props, tmp_path
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        csv_resource = next(r for r in package.resources if r.format == "CSV")
        head, tail = b"col_a,col_b\n1,2\n", b"3,4\n"
        transport = _FlakyTransport(head, tail)

        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(httpx.ReadError):
                await download_resource(
                    client,
                    package,
                    resource_id=csv_resource.id,
                    dest_dir=tmp_path,
                )
            # Second attempt must not short-circuit on a partial file.
            path = await download_resource(
                client,
                package,
                resource_id=csv_resource.id,
                dest_dir=tmp_path,
            )

        assert path.read_bytes() == head + tail

    @pytest.mark.asyncio
    async def test_http_error_leaves_no_file(
        self, saude_dataset_page_props, tmp_path
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        csv_resource = next(r for r in package.resources if r.format == "CSV")

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as client:
            with pytest.raises(httpx.HTTPStatusError):
                await download_resource(
                    client,
                    package,
                    resource_id=csv_resource.id,
                    dest_dir=tmp_path,
                )

        assert list(tmp_path.iterdir()) == []

    @pytest.mark.asyncio
    async def test_complete_download_leaves_no_scratch_file(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        csv_resource = next(r for r in package.resources if r.format == "CSV")
        async with httpx.AsyncClient(transport=mocked_saude) as client:
            await download_resource(
                client,
                package,
                resource_id=csv_resource.id,
                dest_dir=tmp_path,
            )
        assert not list(tmp_path.glob(".*.part"))


class TestDownloadDatasetEdgeCases:
    @pytest.mark.asyncio
    async def test_no_dest_dir(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        original_dir = tmp_path / "cwd"
        original_dir.mkdir()

        import os

        old_cwd = os.getcwd()
        try:
            os.chdir(original_dir)
            async with httpx.AsyncClient(transport=mocked_saude) as client:
                paths = await download_dataset(
                    client,
                    package,
                    fmt="CSV",
                )
            assert len(paths) > 0
        finally:
            os.chdir(old_cwd)

    @pytest.mark.asyncio
    async def test_progress_forwarded(
        self, saude_dataset_page_props, tmp_path, mocked_saude
    ):
        from pysus.api.saude.resources import CKANPackage

        package = CKANPackage.model_validate(saude_dataset_page_props)
        calls = []

        def progress(downloaded: int, total: int):
            calls.append((downloaded, total))

        async with httpx.AsyncClient(transport=mocked_saude) as client:
            await download_dataset(
                client,
                package,
                dest_dir=tmp_path,
                fmt="CSV",
                progress=progress,
            )
        assert len(calls) > 0
