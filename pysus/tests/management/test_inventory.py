"""Tests for pysus.management.inventory collectors (mocked clients)."""

from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pysus.management.inventory import Inventory, _record_changed, _safe_modify
from pysus.management.records import FileRecord


@pytest.fixture
def inventory():
    pysus = MagicMock()
    return Inventory(pysus=pysus)


def _awaitable(value):
    """An awaitable attribute (AsyncMock attrs cannot be awaited directly)."""

    class _Aw:
        def __await__(self):
            async def _inner():
                return value

            return _inner().__await__()

    return _Aw()


_DATASET = None


def _fake_dataset():
    from pysus.api.ftp.client import FTP
    from pysus.api.ftp.models import Dataset

    global _DATASET
    if _DATASET is None:

        class _FakeDataset(Dataset):
            @property
            def name(self):
                return "SINAN"

            @property
            def long_name(self):
                return "Sistema de Informacao de Agravos"

            @property
            def description(self):
                return ""

            def formatter(self, filename):
                return {}

        _DATASET = _FakeDataset(client=FTP())
    return _DATASET


def _ftp_file(name="DENGBR25.dbc"):
    from pysus.api.ftp.models import File as FTPFile

    info = {
        "name": name,
        "path": f"/dissemin/publicos/SINAN/{name}",
        "size": 100,
        "modify": datetime(2026, 1, 1),
        "type": "file",
        "group": {"name": "DENG", "long_name": "Dengue"},
        "year": 2025,
        "month": None,
        "state": None,
    }
    return FTPFile(
        path=info["path"],
        dataset=_fake_dataset(),
        type=info["type"],
        _info=info,
    )


class TestSafeModify:
    def test_modify_available(self):
        file = MagicMock()
        file.modify = datetime(2026, 1, 1)
        assert _safe_modify(file) == datetime(2026, 1, 1)

    def test_modify_raises(self):
        file = MagicMock()
        type(file).modify = property(
            lambda self: (_ for _ in ()).throw(ValueError("nope"))
        )
        assert _safe_modify(file) is None


class TestRecordChanged:
    def test_size_change(self):
        a = FileRecord(
            origin="ftp", dataset="X", name="a.dbc", path="p", size=1
        )
        b = FileRecord(
            origin="ftp", dataset="X", name="a.dbc", path="p", size=2
        )
        assert _record_changed(a, b)

    def test_modify_change(self):
        a = FileRecord(
            origin="ftp",
            dataset="X",
            name="a.dbc",
            path="p",
            size=1,
            modified=datetime(2026, 1, 1),
        )
        b = FileRecord(
            origin="ftp",
            dataset="X",
            name="a.dbc",
            path="p",
            size=1,
            modified=datetime(2026, 1, 2),
        )
        assert _record_changed(a, b)

    def test_identical(self):
        a = FileRecord(
            origin="ftp",
            dataset="X",
            name="a.dbc",
            path="p",
            size=1,
            modified=datetime(2026, 1, 1),
        )
        b = FileRecord(
            origin="ftp",
            dataset="X",
            name="a.dbc",
            path="p",
            size=1,
            modified=datetime(2026, 1, 1),
        )
        assert not _record_changed(a, b)


class TestWalkFtpItem:
    @pytest.mark.asyncio
    async def test_ftp_file(self, inventory):
        file = _ftp_file()
        records = await inventory._walk_ftp_item(file)
        assert len(records) == 1
        assert records[0].origin == "ftp"
        assert records[0].group == "DENG"
        assert records[0].year == 2025
        assert records[0].file is file

    @pytest.mark.asyncio
    async def test_group_walk(self, inventory):
        from pysus.api.models import BaseRemoteGroup

        group = MagicMock(spec=BaseRemoteGroup)
        group.files = [AsyncMock()]
        group.files = _awaitable((_ftp_file(), _ftp_file("DENGBR24.dbc")))
        records = await inventory._walk_ftp_item(group)
        assert len(records) == 2

    @pytest.mark.asyncio
    async def test_directory_walk(self, inventory):
        from pysus.api.ftp.models import Directory

        directory = Directory("/dissemin/publicos/SINAN")
        directory.loaded = True
        directory._content = [_ftp_file("A.dbc")]
        records = await inventory._walk_ftp_item(directory)
        assert len(records) == 1
        assert records[0].name == "A.dbc"

    @pytest.mark.asyncio
    async def test_unknown_item(self, inventory):
        records = await inventory._walk_ftp_item(object())
        assert records == []


class TestCollectFtp:
    @pytest.mark.asyncio
    async def test_collect_ftp(self, inventory):
        dataset = MagicMock()
        dataset.name = "SINAN"
        dataset.content = _awaitable([_ftp_file()])
        client = MagicMock()
        client.datasets = AsyncMock(return_value=[dataset])
        inventory.pysus.get_ftp = AsyncMock(return_value=client)

        records = await inventory.collect("ftp")
        assert len(records) == 1
        assert records[0].dataset == "SINAN"

    @pytest.mark.asyncio
    async def test_collect_ftp_filtered(self, inventory):
        dataset = MagicMock()
        dataset.name = "SINAN"
        dataset.content = _awaitable([_ftp_file()])
        other = MagicMock()
        other.name = "SIM"
        other.content = _awaitable([])
        client = MagicMock()
        client.datasets = AsyncMock(return_value=[dataset, other])
        inventory.pysus.get_ftp = AsyncMock(return_value=client)

        records = await inventory.collect("ftp", ["SINAN"])
        assert len(records) == 1

    @pytest.mark.asyncio
    async def test_collect_unknown_origin(self, inventory):
        with pytest.raises(ValueError, match="Unknown origin"):
            await inventory.collect("nope")


class TestCollectDadosgov:
    @pytest.mark.asyncio
    async def test_collect_dadosgov(self, inventory):
        from pathlib import Path

        from pysus.api.models import BaseRemoteGroup

        class _FakeGroup(BaseRemoteGroup):
            @property
            def name(self):
                return "DENG"

            @property
            def long_name(self):
                return "Dengue"

            @property
            def description(self):
                return ""

            async def _fetch_files(self):
                return []

        group = _FakeGroup(path=Path("x"), type="file", dataset=_fake_dataset())
        group._files = [_ftp_file("DENGBR25.csv.zip")]
        dataset = MagicMock()
        dataset.name = "SINAN"
        dataset.content = _awaitable([group])
        client = MagicMock()
        client.datasets = AsyncMock(return_value=[dataset])
        inventory.pysus.get_dadosgov = AsyncMock(return_value=client)

        record = FileRecord(
            origin="dadosgov",
            dataset="SINAN",
            name="DENGBR25.csv.zip",
            path="http://x",
            group="DENG",
            year=2025,
        )
        with patch.object(
            Inventory,
            "_walk_ftp_item",
            new=AsyncMock(return_value=[record]),
        ):
            records = await inventory.collect("dadosgov", dadosgov_token="tok")
        assert len(records) == 1
        assert records[0].origin == "dadosgov"


class TestCollectDucklake:
    @pytest.mark.asyncio
    async def test_collect_ducklake(self, inventory):
        record = MagicMock()
        record.rows = 10
        record.sha256 = "a" * 64
        record.year = 2025
        record.month = None
        record.state = None
        record.modified = datetime(2026, 1, 1)
        record.origin_path = "/ftp/x.dbc"
        record.origin_size = 50
        record.origin_modified = datetime(2026, 1, 1)
        record.group = None

        file = MagicMock()
        file.basename = "X.parquet"
        file.path = "public/data/ftp/sinan/X.parquet"
        file.size = 100
        file.record = record

        dataset = MagicMock()
        dataset.name = "sinan"
        dataset.query = AsyncMock(return_value=[file])
        client = MagicMock()
        client.datasets = AsyncMock(return_value=[dataset])
        inventory.pysus.get_ducklake = AsyncMock(return_value=client)

        records = await inventory.collect("ducklake")
        assert len(records) == 1
        assert records[0].sha256 == "a" * 64
        assert records[0].source_path == "/ftp/x.dbc"


class TestCollectSaude:
    @pytest.mark.asyncio
    async def test_collect_saude_endpoint_file(self, inventory):
        """Endpoint files become FileRecords with format=jsonl."""
        from pysus.api.saude.databases import DatasetSpec
        from pysus.api.saude.models import SaudeDataset, SaudeEndpointFile
        from pysus.api.saude.rest import EndpointSpec

        ep_spec = EndpointSpec(
            path="/arboviroses/dengue",
            summary="Dengue data",
            tag="Agravo Arboviroses",
        )
        endpoint_file = SaudeEndpointFile(
            record=ep_spec,
            dataset=SaudeDataset.model_construct(
                spec=DatasetSpec(
                    name="ARBOVIROSES",
                    long_name="Arboviroses",
                    description="",
                    ckan_group="arboviroses",
                    slug_patterns=(),
                    exclude_patterns=(),
                    demas_tags=(),
                    endpoints=(),
                ),
                client=MagicMock(),
            ),
            path=Path("/tmp/arboviroses_dengue"),
        )

        ds_spec = DatasetSpec(
            name="ARBOVIROSES",
            long_name="Arboviroses",
            description="",
            ckan_group="arboviroses",
            slug_patterns=(),
            exclude_patterns=(),
            demas_tags=("Agravo Arboviroses",),
            endpoints=("/arboviroses/dengue",),
        )
        dataset = MagicMock(spec=SaudeDataset)
        dataset.name = "ARBOVIROSES"
        dataset.spec = ds_spec
        dataset.content = _awaitable([endpoint_file])

        client = MagicMock()
        client.datasets = AsyncMock(return_value=[dataset])
        inventory.pysus.get_saude = AsyncMock(return_value=client)

        records = await inventory.collect("saude")
        assert len(records) == 1
        assert records[0].origin == "saude"
        assert records[0].dataset == "ARBOVIROSES"
        assert records[0].format == "jsonl"
        assert records[0].name == "arboviroses_dengue.jsonl"

    @pytest.mark.asyncio
    async def test_collect_saude_ckan_group(self, inventory):
        """CKAN groups produce records for each resource file."""
        from pysus.api.saude.models import SaudeDataset, SaudeGroup

        group = MagicMock(spec=SaudeGroup)
        group.name = "arboviroses-dengue"

        file = MagicMock()
        file.basename = "DENGBR25.csv.zip"
        file.path = Path("https://dadosabertos.saude.gov.br/resources/dengue")
        file.size = 1024
        file.year = 2025
        file.month = None
        file.state = None

        group.files = _awaitable([file])

        ds_spec = MagicMock()
        ds_spec.name = "ARBOVIROSES"
        dataset = MagicMock(spec=SaudeDataset)
        dataset.name = "ARBOVIROSES"
        dataset.spec = ds_spec
        dataset.content = _awaitable([group])

        client = MagicMock()
        client.datasets = AsyncMock(return_value=[dataset])
        inventory.pysus.get_saude = AsyncMock(return_value=client)

        records = await inventory.collect("saude")
        assert len(records) == 1
        assert records[0].origin == "saude"
        assert records[0].name == "DENGBR25.csv.zip"
        assert records[0].group == "ARBOVIROSES-DENGUE"
        assert records[0].year == 2025

    @pytest.mark.asyncio
    async def test_collect_saude_filtered(self, inventory):
        """Dataset filter excludes non-matching datasets."""
        from pysus.api.saude.models import SaudeDataset

        ds1 = MagicMock(spec=SaudeDataset)
        ds1.name = "ARBOVIROSES"
        ds1.content = _awaitable([])
        ds2 = MagicMock(spec=SaudeDataset)
        ds2.name = "CNES"
        ds2.content = _awaitable([])

        client = MagicMock()
        client.datasets = AsyncMock(return_value=[ds1, ds2])
        inventory.pysus.get_saude = AsyncMock(return_value=client)

        records = await inventory.collect("saude", ["ARBOVIROSES"])
        assert len(records) == 0  # content is empty, but filter worked

    @pytest.mark.asyncio
    async def test_collect_saude_mixed_content(self, inventory):
        """Mixed content (groups + endpoint files) are all collected."""
        from pysus.api.saude.databases import DatasetSpec
        from pysus.api.saude.models import (
            SaudeDataset,
            SaudeEndpointFile,
            SaudeGroup,
        )
        from pysus.api.saude.rest import EndpointSpec

        ep_spec = EndpointSpec(path="/cnes/estabelecimentos")
        ep_file = SaudeEndpointFile(
            record=ep_spec,
            dataset=SaudeDataset.model_construct(
                spec=DatasetSpec(
                    name="CNES",
                    long_name="CNES",
                    description="",
                    ckan_group=None,
                    slug_patterns=(),
                    exclude_patterns=(),
                    demas_tags=(),
                    endpoints=(),
                ),
                client=MagicMock(),
            ),
            path=Path("/tmp/cnes_estabelecimentos"),
        )

        group = MagicMock(spec=SaudeGroup)
        group.name = "cnes-estabelecimentos"
        file = MagicMock()
        file.basename = "estabelecimentos.csv"
        file.path = Path("https://x/estabelecimentos.csv")
        file.size = 500
        file.year = None
        file.month = None
        file.state = None
        group.files = _awaitable([file])

        ds_spec = MagicMock()
        ds_spec.name = "CNES"
        dataset = MagicMock(spec=SaudeDataset)
        dataset.name = "CNES"
        dataset.spec = ds_spec
        dataset.content = _awaitable([ep_file, group])

        client = MagicMock()
        client.datasets = AsyncMock(return_value=[dataset])
        inventory.pysus.get_saude = AsyncMock(return_value=client)

        records = await inventory.collect("saude")
        assert len(records) == 2
        formats = {r.format for r in records}
        assert "jsonl" in formats
        assert "csv" in formats


class TestCollectAllIncludesSaude:
    @pytest.mark.asyncio
    async def test_collect_all_returns_four_origins(self, inventory):
        """collect_all should include saude in the returned dict."""
        # Stub all collectors to return empty lists
        inventory._collect_ftp = AsyncMock(return_value=[])
        inventory._collect_dadosgov = AsyncMock(return_value=[])
        inventory._collect_ducklake = AsyncMock(return_value=[])
        inventory._collect_saude = AsyncMock(return_value=[])

        result = await inventory.collect_all()
        assert "saude" in result
        assert len(result) == 4


class TestSaudeFormats:
    def test_jsonl_format_detected(self):
        """FileRecord auto-detects jsonl format from name."""
        r = FileRecord(
            origin="saude",
            dataset="X",
            name="arboviroses_dengue.jsonl",
            path="https://x",
        )
        assert r.format == "jsonl"

    def test_jsonl_stem_matches_dadosgov(self):
        """Cross-origin stem normalisation: Saude endpoint vs DadosGov slug."""
        from pysus.management.records import stem_of

        # Saude: arboviroses_dengue.jsonl → stem = arboviroses_dengue
        # DadosGov: arboviroses-dengue → stem = arboviroses_dengue
        assert stem_of("arboviroses_dengue.jsonl") == stem_of(
            "arboviroses-dengue"
        )

    def test_jsonl_stem_matches_ckan(self):
        """Cross-origin stem: Saude CKAN resource = DadosGov CKAN resource."""
        from pysus.management.records import stem_of

        assert stem_of("DENGBR25.csv.zip") == stem_of("DENGBR25.csv.zip")
