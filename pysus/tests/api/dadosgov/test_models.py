"""Tests for pysus.api.dadosgov.models."""

import asyncio
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pysus import CACHEPATH
from pysus.api.dadosgov.client import ConjuntoDados, DadosGov, Recurso
from pysus.api.dadosgov.models import Dataset, File, Group, _dedup_entries

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_recurso(**overrides) -> Recurso:
    kwargs = {
        "id": "r1",
        "titulo": "Resource",
        "link": "http://example.com/file.csv",
        "tamanho": 100,
        "dataUltimaAtualizacaoArquivo": "01/01/2024",
    }
    kwargs.update(overrides)
    return Recurso(**kwargs)  # type: ignore[arg-type]


def make_conjunto(resources=None) -> ConjuntoDados:
    if resources is None:
        resources = [make_recurso()]
    return ConjuntoDados(
        id="c1",
        titulo="Conjunto Teste",
        nome="conjunto-teste",
        recursos=resources,
    )


class MockDataset(Dataset):
    group_aliases: dict = {}

    @property
    def name(self) -> str:
        return "TestDS"

    @property
    def long_name(self) -> str:
        return "Test Dataset"

    @property
    def description(self) -> str:
        return "A test dataset"

    async def _fetch_content(self):
        return await super()._fetch_content()

    def formatter(self, filename: str) -> dict:
        return {}


# ---------------------------------------------------------------------------
# _dedup_entries
# ---------------------------------------------------------------------------


class TestDedupEntries:
    def test_prefers_csv_over_json_xml(self):
        entries = [
            ("data.csv", "csv_obj", {"fmt": "csv"}),
            ("data.json", "json_obj", {"fmt": "json"}),
            ("data.xml", "xml_obj", {"fmt": "xml"}),
        ]
        result = _dedup_entries(entries)
        assert len(result) == 1
        assert result[0][0] == "data.csv"

    def test_multiple_stems(self):
        entries = [
            ("a.csv", "a_csv", {}),
            ("a.json", "a_json", {}),
            ("b.csv", "b_csv", {}),
        ]
        result = _dedup_entries(entries)
        assert len(result) == 2
        filenames = {r[0] for r in result}
        assert filenames == {"a.csv", "b.csv"}

    def test_no_format_match_returns_all(self):
        entries = [("readme.txt", "t1", {}), ("notes.md", "t2", {})]
        result = _dedup_entries(entries)
        assert len(result) == 2

    def test_single_entry(self):
        entries = [("data.csv", "obj", {})]
        result = _dedup_entries(entries)
        assert result == entries

    def test_zip_format_detection(self):
        entries = [("data.csv.zip", "cz", {}), ("data.json.zip", "jz", {})]
        result = _dedup_entries(entries)
        assert len(result) == 1
        assert result[0][0] == "data.csv.zip"

    def test_only_json_and_xml_no_csv(self):
        entries = [("data.json", "j", {}), ("data.xml", "x", {})]
        result = _dedup_entries(entries)
        assert len(result) == 2

    def test_empty_list(self):
        assert _dedup_entries([]) == []


# ---------------------------------------------------------------------------
# File
# ---------------------------------------------------------------------------


class TestFileInit:
    def test_init_with_metadata(self):
        recurso = make_recurso()
        ds = MockDataset(client=DadosGov())
        conj = make_conjunto([recurso])
        group = Group(record=conj, dataset=ds)
        f = File(
            record=recurso,
            dataset=ds,
            group=group,
            path=recurso.url,
            _metadata={"year": 2024, "month": 1, "state": "SP"},
        )
        assert f.record is recurso
        assert f.dataset is ds
        assert f.group is group
        assert f.year == 2024
        assert f.month == 1
        assert f.state == "SP"

    def test_init_without_metadata(self):
        recurso = make_recurso()
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path=recurso.url)
        assert f._metadata == {}

    def test_repr_returns_basename(self):
        recurso = make_recurso()
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path="http://example.com/data.csv")
        assert repr(f) == "data.csv"


class TestFileModelPostInit:
    def test_with_api_size_and_last_modified_no_task(self):
        recurso = make_recurso(tamanho=100)
        ds = MockDataset(client=DadosGov())

        with patch.object(asyncio, "get_running_loop") as mock_loop:
            File(record=recurso, dataset=ds, path=recurso.url)
        mock_loop.create_task.assert_not_called()

    def test_without_api_size_creates_task(self):
        recurso = make_recurso(tamanho=0)
        ds = MockDataset(client=DadosGov())
        mock_loop = MagicMock()

        with patch.object(asyncio, "get_running_loop", return_value=mock_loop):
            File(record=recurso, dataset=ds, path=recurso.url)
        mock_loop.create_task.assert_called_once()

    def test_without_last_modified_creates_task(self):
        recurso = make_recurso(dataUltimaAtualizacaoArquivo="Indisponível")
        ds = MockDataset(client=DadosGov())
        mock_loop = MagicMock()

        with patch.object(asyncio, "get_running_loop", return_value=mock_loop):
            File(record=recurso, dataset=ds, path=recurso.url)
        mock_loop.create_task.assert_called_once()

    def test_no_event_loop_runtime_error_swallowed(self):
        recurso = make_recurso(tamanho=0)
        ds = MockDataset(client=DadosGov())

        def _raise():
            raise RuntimeError("No running event loop")

        with patch.object(asyncio, "get_running_loop", side_effect=_raise):
            File(record=recurso, dataset=ds, path=recurso.url)


class TestFileProperties:
    def test_extension_from_file_name(self):
        recurso = make_recurso(nomeArquivo="dados.csv")
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path=recurso.url)
        assert f.extension == ".csv"

    def test_extension_from_url_when_no_file_name(self):
        recurso = make_recurso(
            nomeArquivo=None, link="http://example.com/arquivo.zip"
        )
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path=recurso.url)
        assert f.extension == ".zip"

    def test_extension_from_url_with_query_string(self):
        recurso = make_recurso(
            nomeArquivo=None, link="http://example.com/arquivo.csv?download=1"
        )
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path=recurso.url)
        assert f.extension == ".csv"

    def test_size_from_api_size(self):
        recurso = make_recurso(tamanho=500)
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path=recurso.url)
        assert f.size == 500

    def test_size_zero_when_no_api_size(self):
        recurso = make_recurso(tamanho=0)
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path=recurso.url)
        assert f.size == 0

    def test_modify_returns_datetime(self):
        recurso = make_recurso()
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path=recurso.url)
        assert isinstance(f.modify, datetime)

    def test_modify_raises_value_error_when_none(self):
        recurso = make_recurso(dataUltimaAtualizacaoArquivo="Indisponível")
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path=recurso.url)
        with pytest.raises(ValueError, match="File requires a modify date"):
            f.modify

    def test_year_month_state_from_metadata(self):
        recurso = make_recurso()
        ds = MockDataset(client=DadosGov())
        f = File(
            record=recurso,
            dataset=ds,
            path=recurso.url,
            _metadata={"year": 2023, "month": 6, "state": "RJ"},
        )
        assert f.year == 2023
        assert f.month == 6
        assert f.state == "RJ"

    def test_year_month_state_defaults_to_none(self):
        recurso = make_recurso()
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path=recurso.url)
        assert f.year is None
        assert f.month is None
        assert f.state is None


class TestFileFetchMetadata:
    @pytest.mark.asyncio
    async def test_head_success_updates_record(self):
        recurso = make_recurso(
            tamanho=0, dataUltimaAtualizacaoArquivo="Indisponível"
        )
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path="http://example.com/file.csv")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {
            "Content-Length": "999",
            "Last-Modified": "Mon, 15 Jan 2024 10:30:00 GMT",
        }

        mock_client = AsyncMock()
        mock_client.head.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client

        with patch("httpx.AsyncClient", return_value=mock_client):
            await f.fetch_metadata()

        assert f.record.api_size == 999
        assert f.record.last_modified is not None

    @pytest.mark.asyncio
    async def test_head_405_fallback_to_get(self):
        recurso = make_recurso(tamanho=0)
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path="http://example.com/file.csv")

        head_response = MagicMock()
        head_response.status_code = 405

        get_response = MagicMock()
        get_response.headers = {
            "Content-Length": "777",
            "Last-Modified": "Tue, 01 Feb 2024 00:00:00 GMT",
        }

        mock_client = AsyncMock()
        mock_client.head.return_value = head_response
        mock_client.get.return_value = get_response
        mock_client.__aenter__.return_value = mock_client

        with patch("httpx.AsyncClient", return_value=mock_client):
            await f.fetch_metadata()

        assert f.record.api_size == 777

        called_args, called_kwargs = mock_client.get.call_args
        actual_url = Path(called_args[0]).as_posix()

        assert actual_url in (
            "http:/example.com/file.csv",
            "http://example.com/file.csv",
        )
        assert called_kwargs == {"headers": {"Range": "bytes=0-0"}}

    @pytest.mark.asyncio
    async def test_no_content_length_header(self):
        recurso = make_recurso(tamanho=0)
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path="http://example.com/file.csv")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}

        mock_client = AsyncMock()
        mock_client.head.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client

        with patch("httpx.AsyncClient", return_value=mock_client):
            await f.fetch_metadata()

        assert f.record.api_size == 0

    @pytest.mark.asyncio
    async def test_exception_is_caught(self):
        recurso = make_recurso(tamanho=0)
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path="http://example.com/file.csv")

        mock_client = AsyncMock()
        mock_client.head.side_effect = Exception("Network error")
        mock_client.__aenter__.return_value = mock_client

        with patch("httpx.AsyncClient", return_value=mock_client):
            await f.fetch_metadata()

        assert f.record.api_size == 0

    @pytest.mark.asyncio
    async def test_parse_typeerror_is_caught(self):
        recurso = make_recurso(tamanho=0)
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path="http://example.com/file.csv")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {
            "Content-Length": "100",
            "Last-Modified": "invalid-date-string",
        }

        mock_client = AsyncMock()
        mock_client.head.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client

        with patch("httpx.AsyncClient", return_value=mock_client):
            with patch(
                "pysus.api.dadosgov.models.parse", side_effect=TypeError
            ):
                await f.fetch_metadata()

        assert f.record.api_size == 100
        assert f.record.last_modified == datetime(2024, 1, 1)

    @pytest.mark.asyncio
    async def test_parse_valueerror_is_caught(self):
        recurso = make_recurso(tamanho=0)
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path="http://example.com/file.csv")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {
            "Content-Length": "100",
            "Last-Modified": "invalid-date-string",
        }

        mock_client = AsyncMock()
        mock_client.head.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client

        with patch("httpx.AsyncClient", return_value=mock_client):
            with patch(
                "pysus.api.dadosgov.models.parse", side_effect=ValueError
            ):
                await f.fetch_metadata()

        assert f.record.api_size == 100
        assert f.record.last_modified == datetime(2024, 1, 1)


class TestFileDownload:
    @pytest.mark.asyncio
    async def test_download_delegates_to_client(self):
        recurso = make_recurso()
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, group=None, path=recurso.url)

        output = Path("/tmp/test_out.csv")
        callback = MagicMock()

        with patch.object(
            ds.client, "_download_file", new_callable=AsyncMock
        ) as mock_dl:
            mock_dl.return_value = output
            result = await f._download(output=output, callback=callback)

        assert result == output
        mock_dl.assert_awaited_once_with(f, output, callback=callback)

    @pytest.mark.asyncio
    async def test_download_default_output(self):
        recurso = make_recurso()
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path=recurso.url)

        expected = CACHEPATH / f.name

        with patch.object(
            ds.client, "_download_file", new_callable=AsyncMock
        ) as mock_dl:
            mock_dl.return_value = expected
            result = await f._download()

        assert result == expected


class TestFileFetchSize:
    @pytest.mark.asyncio
    async def test_head_success_updates_and_returns_size(self):
        recurso = make_recurso(tamanho=0)
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path="http://example.com/file.csv")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Length": "1234"}

        mock_client = AsyncMock()
        mock_client.head.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client

        with patch("httpx.AsyncClient", return_value=mock_client):
            size = await f.fetch_size()

        assert size == 1234
        assert f.record.api_size == 1234

    @pytest.mark.asyncio
    async def test_head_405_fallback_to_get(self):
        recurso = make_recurso(tamanho=0)
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path="http://example.com/file.csv")

        head_response = MagicMock()
        head_response.status_code = 405

        get_response = MagicMock()
        get_response.headers = {"Content-Length": "5678"}

        mock_client = AsyncMock()
        mock_client.head.return_value = head_response
        mock_client.get.return_value = get_response
        mock_client.__aenter__.return_value = mock_client

        with patch("httpx.AsyncClient", return_value=mock_client):
            size = await f.fetch_size()

        assert size == 5678
        assert f.record.api_size == 5678

        called_args, called_kwargs = mock_client.get.call_args
        actual_url = Path(called_args[0]).as_posix()

        assert actual_url in (
            "http:/example.com/file.csv",
            "http://example.com/file.csv",
        )
        assert called_kwargs == {"headers": {"Range": "bytes=0-0"}}

    @pytest.mark.asyncio
    async def test_head_returns_zero_content_length(self):
        recurso = make_recurso(tamanho=0)
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path="http://example.com/file.csv")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Length": "0"}

        mock_client = AsyncMock()
        mock_client.head.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client

        with patch("httpx.AsyncClient", return_value=mock_client):
            size = await f.fetch_size()

        assert size == 0
        assert f.record.api_size == 0

    @pytest.mark.asyncio
    async def test_exception_returns_zero(self):
        recurso = make_recurso(tamanho=0)
        ds = MockDataset(client=DadosGov())
        f = File(record=recurso, dataset=ds, path="http://example.com/file.csv")

        mock_client = AsyncMock()
        mock_client.head.side_effect = Exception("Timeout")
        mock_client.__aenter__.return_value = mock_client

        with patch("httpx.AsyncClient", return_value=mock_client):
            size = await f.fetch_size()

        assert size == 0


# ---------------------------------------------------------------------------
# Group
# ---------------------------------------------------------------------------


class TestGroupInit:
    def test_init_with_formatter(self):
        ds = MockDataset(client=DadosGov())
        conj = make_conjunto()

        def formatter(fn):
            return {"year": 2024}

        g = Group(record=conj, dataset=ds, formatter=formatter)
        assert g.record is conj
        assert g.dataset is ds
        assert g._formatter is formatter

    def test_init_without_formatter(self):
        ds = MockDataset(client=DadosGov())
        conj = make_conjunto()
        g = Group(record=conj, dataset=ds)
        assert g._formatter is None

    def test_repr_returns_name(self):
        ds = MockDataset(client=DadosGov())
        conj = make_conjunto()
        g = Group(record=conj, dataset=ds)
        assert repr(g) == g.name


class TestGroupProperties:
    def test_name_with_aliases(self):
        ds = MockDataset(client=DadosGov())
        ds.group_aliases = {"conjunto-teste": "CT"}
        conj = make_conjunto()
        g = Group(record=conj, dataset=ds)
        assert g.name == "CT"

    def test_name_without_aliases(self):
        ds = MockDataset(client=DadosGov())
        ds.group_aliases = {}
        conj = make_conjunto()
        g = Group(record=conj, dataset=ds)
        assert g.name == "conjunto-teste"

    def test_long_name(self):
        ds = MockDataset(client=DadosGov())
        conj = make_conjunto()
        g = Group(record=conj, dataset=ds)
        assert g.long_name == "Conjunto Teste"

    def test_description(self):
        ds = MockDataset(client=DadosGov())
        conj = make_conjunto()
        g = Group(record=conj, dataset=ds)
        assert g.description == ""


class TestGroupFetchFiles:
    @pytest.mark.asyncio
    async def test_filters_pdf_and_get_prefix(self):
        resources = [
            make_recurso(
                id="r1", link="http://ex.com/doc.pdf", nomeArquivo="doc.pdf"
            ),
            make_recurso(
                id="r2",
                link="http://ex.com/get_data.csv",
                nomeArquivo="get_data.csv",
            ),
            make_recurso(
                id="r3", link="http://ex.com/data.csv", nomeArquivo="data.csv"
            ),
        ]
        conj = make_conjunto(resources)
        ds = MockDataset(client=DadosGov())
        g = Group(record=conj, dataset=ds)

        files = await g._fetch_files()

        assert len(files) == 1
        assert files[0].record.id == "r3"

    @pytest.mark.asyncio
    async def test_deduplicates_preferring_csv(self):
        resources = [
            make_recurso(
                id="r1", link="http://ex.com/data.csv", nomeArquivo="data.csv"
            ),
            make_recurso(
                id="r2", link="http://ex.com/data.json", nomeArquivo="data.json"
            ),
            make_recurso(
                id="r3", link="http://ex.com/data.xml", nomeArquivo="data.xml"
            ),
        ]
        conj = make_conjunto(resources)
        ds = MockDataset(client=DadosGov())
        g = Group(record=conj, dataset=ds)

        files = await g._fetch_files()

        assert len(files) == 1
        assert files[0].record.id == "r1"

    @pytest.mark.asyncio
    async def test_formatter_applied(self):
        resources = [
            make_recurso(
                id="r1",
                link="http://ex.com/SP2024.csv",
                nomeArquivo="SP2024.csv",
            ),
        ]
        conj = make_conjunto(resources)
        ds = MockDataset(client=DadosGov())

        def formatter(fn):
            return {"state": "SP", "year": 2024}

        g = Group(record=conj, dataset=ds, formatter=formatter)

        files = await g._fetch_files()

        assert len(files) == 1
        assert files[0].state == "SP"
        assert files[0].year == 2024

    @pytest.mark.asyncio
    async def test_formatter_not_implemented_error_caught(self):
        resources = [
            make_recurso(
                id="r1", link="http://ex.com/data.csv", nomeArquivo="data.csv"
            ),
        ]
        conj = make_conjunto(resources)
        ds = MockDataset(client=DadosGov())

        def bad_formatter(fn):
            raise NotImplementedError("not implemented")

        g = Group(record=conj, dataset=ds, formatter=bad_formatter)

        files = await g._fetch_files()

        assert len(files) == 1
        assert files[0].state is None

    @pytest.mark.asyncio
    async def test_filename_from_url_when_no_file_name(self):
        resources = [
            make_recurso(
                id="r1",
                nomeArquivo=None,
                link="http://ex.com/download?file=data.csv",
            ),
        ]
        conj = make_conjunto(resources)
        ds = MockDataset(client=DadosGov())
        g = Group(record=conj, dataset=ds)

        files = await g._fetch_files()
        assert len(files) == 1
        assert "download" in str(files[0].path)


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


class TestDatasetContent:
    @pytest.mark.asyncio
    async def test_fetch_content_with_ids(self):
        client = DadosGov()
        ds = MockDataset(client=client)
        ds.ids = ["id1", "id2"]

        conj1 = make_conjunto([make_recurso(id="r1")])
        conj2 = make_conjunto([make_recurso(id="r2")])

        with patch(
            "pysus.api.dadosgov.client.DadosGov.get_dataset",
            new_callable=AsyncMock,
        ) as mock_get:
            mock_get.side_effect = [conj1, conj2]
            groups = await ds._fetch_content()

        assert len(groups) == 2
        assert groups[0].record is conj1
        assert groups[1].record is conj2
        assert callable(groups[0]._formatter)
        assert groups[0]._formatter("x") == ds.formatter("x")
        mock_get.assert_any_call("id1")
        mock_get.assert_any_call("id2")

    @pytest.mark.asyncio
    async def test_fetch_content_empty_ids(self):
        ds = MockDataset(client=DadosGov())
        ds.ids = []

        with patch(
            "pysus.api.dadosgov.client.DadosGov.get_dataset"
        ) as mock_get:
            groups = await ds._fetch_content()

        assert groups == []
        mock_get.assert_not_called()

    def test_repr_returns_name(self):
        ds = MockDataset(client=DadosGov())
        assert repr(ds) == "TestDS"

    def test_abstract_formatter_pass(self):
        class DirectDataset(Dataset):
            ids: list[str] = ["abc"]

            @property
            def name(self):
                return "test"

            @property
            def long_name(self):
                return "Test"

            @property
            def description(self):
                return "Test dataset"

            def formatter(self, filename):
                Dataset.formatter(self, filename)
                return {}

        ds = DirectDataset(client=DadosGov())
        assert ds.formatter("x.csv") == {}

    def test_formatter_not_abstract(self):
        ds = MockDataset(client=DadosGov())
        assert ds.formatter("any.csv") == {}
