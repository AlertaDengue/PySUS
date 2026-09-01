"""Tests for Saude integration in sync engine (5.E)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pysus.management.records import FileRecord
from pysus.management.sync import SyncEngine


def _record(origin, name, dataset="SINAN", year=2025, **kw):
    return FileRecord(
        origin=origin,
        dataset=dataset,
        name=name,
        path=f"{origin}/{dataset}/{name}",
        year=year,
        file=kw.pop("file", MagicMock()),
        **kw,
    )


class TestOriginsFilter:
    def _make_engine_with_mock_inventory(self, collect_side_effect):
        engine = SyncEngine(access_key="ak", secret_key="sk")
        mock_inv = MagicMock()
        mock_inv.collect = AsyncMock(side_effect=collect_side_effect)

        ducklake = MagicMock()
        ducklake.catalog_adapter.ensure_connected = AsyncMock()
        ducklake.catalog_adapter.connect = AsyncMock()
        ducklake.columns_adapter.ensure_connected = AsyncMock()
        ducklake.columns_adapter.connect = AsyncMock()
        ducklake.get_dataset_adapter.return_value = MagicMock(
            ensure_connected=AsyncMock()
        )
        engine._ducklake = ducklake
        return engine, mock_inv

    @pytest.mark.asyncio
    async def test_origins_none_collects_all_with_token(self):
        called = []

        async def fake_collect(origin, **kw):
            called.append(origin)
            return []

        engine, mock_inv = self._make_engine_with_mock_inventory(fake_collect)
        engine.dadosgov_token = "fake_token"

        with patch.object(engine, "_require_pysus", return_value=MagicMock()):
            with patch(
                "pysus.management.sync.Inventory", return_value=mock_inv
            ):
                with patch.object(
                    engine, "_dedupe_s3_artifacts", new=AsyncMock()
                ):
                    with patch.object(
                        engine, "_fix_misparsed_metadata", new=AsyncMock()
                    ):
                        await engine.run(origins=None)

        assert "ducklake" in called
        assert "ftp" in called
        assert "dadosgov" in called
        assert "saude" in called

    @pytest.mark.asyncio
    async def test_saude_only_skips_ftp_and_dadosgov(self):
        called = []

        async def fake_collect(origin, **kw):
            called.append(origin)
            return []

        engine, mock_inv = self._make_engine_with_mock_inventory(fake_collect)

        with patch.object(engine, "_require_pysus", return_value=MagicMock()):
            with patch(
                "pysus.management.sync.Inventory", return_value=mock_inv
            ):
                with patch.object(
                    engine, "_dedupe_s3_artifacts", new=AsyncMock()
                ):
                    with patch.object(
                        engine, "_fix_misparsed_metadata", new=AsyncMock()
                    ):
                        await engine.run(origins=("ducklake", "saude"))

        assert "ducklake" in called
        assert "ftp" not in called
        assert "dadosgov" not in called
        assert "saude" in called

    def test_origins_filter_logic(self):
        all_origins = ("ducklake", "ftp", "dadosgov", "saude")
        active = None or all_origins
        assert active == ("ducklake", "ftp", "dadosgov", "saude")

        active = ("ducklake", "saude")
        assert "ducklake" in active
        assert "saude" in active
        assert "ftp" not in active
        assert "dadosgov" not in active


class TestS3KeySaude:
    def test_s3_key_saude_jsonl(self):
        engine = SyncEngine(access_key="ak", secret_key="sk")
        file = MagicMock()
        file.client.name = "saude"
        file.dataset.name = "ARBOVIRUSES_DENGUE"
        file.basename = "arboviroses_dengue.jsonl"
        file.group = MagicMock()
        file.group.name = None
        file.year = None
        file.month = None
        file.state = None
        key = engine.s3_key_for(file)
        assert "saude/" in key
        assert key.endswith(".parquet")

    def test_s3_key_saude_rest_endpoint(self):
        engine = SyncEngine(access_key="ak", secret_key="sk")
        file = MagicMock()
        file.client.name = "saude"
        file.dataset.name = "ARBOVIRUSES_DENGUE"
        file.basename = "endpoints/arboviroses_dengue_uf.jsonl"
        file.group = MagicMock()
        file.group.name = None
        file.year = None
        file.month = None
        file.state = None
        key = engine.s3_key_for(file)
        assert "saude/" in key
        assert key.endswith(".parquet")
