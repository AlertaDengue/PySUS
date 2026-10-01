"""Tests for pysus.management.sync key resolution (no network)."""

from unittest.mock import MagicMock

import pytest
from pysus.management.sync import SyncEngine


class TestSyncEngine:
    def _engine(self):
        return SyncEngine(access_key="ak", secret_key="sk")

    def _file(self, client_name, dataset_name, basename, **attrs):
        file = MagicMock()
        file.client.name = client_name
        file.dataset.name = dataset_name
        file.basename = basename
        group = MagicMock()
        group.name = attrs.get("group")
        file.group = group
        file.year = attrs.get("year")
        file.month = attrs.get("month")
        file.state = attrs.get("state")
        return file

    def test_s3_key_ftp_dbc(self):
        engine = self._engine()
        file = self._file(
            "ftp",
            "SINAN",
            "DENGBR25.dbc",
            group="DENG",
            year=2025,
        )
        assert (
            engine.s3_key_for(file)
            == "public/data/ftp/sinan/DENG/2025/_/BR/DENGBR25.parquet"
        )

    def test_s3_key_dadosgov_csv_zip(self):
        engine = self._engine()
        file = self._file(
            "DadosGov",
            "SINAN",
            "DENGBR25.csv.zip",
            group="DENG",
            year=2025,
        )
        assert (
            engine.s3_key_for(file)
            == "public/data/dadosgov/sinan/DENG/2025/_/BR/DENGBR25.parquet"
        )

    def test_s3_key_dadosgov_json_variant_collides_with_csv(self):
        engine = self._engine()
        csv = self._file(
            "DadosGov",
            "SIM",
            "Mortalidade_Geral_2022_csv.zip",
            group="DO",
            year=2022,
        )
        jsn = self._file(
            "DadosGov",
            "SIM",
            "Mortalidade_Geral_2022.json.zip",
            group="DO",
            year=2022,
        )
        assert engine.s3_key_for(csv) == engine.s3_key_for(jsn)

    def test_s3_key_full_attributes(self):
        engine = self._engine()
        file = self._file(
            "ftp",
            "SIA",
            "PAAC2501.dbc",
            group="PA",
            year=2025,
            month=1,
            state="AC",
        )
        assert (
            engine.s3_key_for(file)
            == "public/data/ftp/sia/PA/2025/01/AC/PAAC2501.parquet"
        )

    def test_is_current(self):
        engine = self._engine()
        from datetime import datetime

        file = MagicMock()
        file.modify = datetime(2026, 1, 2)
        assert engine._is_current(file, datetime(2026, 1, 2))
        assert not engine._is_current(file, datetime(2026, 1, 1))
        assert not engine._is_current(file, None)

    def test_s3_is_stale_when_source_size_differs(self):
        from pysus.management.records import FileComparison, FileRecord

        ftp = FileRecord(
            origin="ftp",
            dataset="SINAN",
            name="DENGBR25.dbc",
            path="ftp/x",
            size=200,
            modified=None,
            group="DENG",
            year=2025,
        )
        s3 = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="s3/x",
            size=5000,
            source_size=100,
            group="DENG",
            year=2025,
        )
        comparison = FileComparison(key=ftp.identity_key(), records=[ftp, s3])
        assert SyncEngine._s3_is_stale(comparison)

    def test_s3_not_stale_when_sizes_equal(self):
        from pysus.management.records import FileComparison, FileRecord

        ftp = FileRecord(
            origin="ftp",
            dataset="SINAN",
            name="DENGBR25.dbc",
            path="ftp/x",
            size=100,
            group="DENG",
            year=2025,
        )
        s3 = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="s3/x",
            size=5000,
            source_size=100,
            group="DENG",
            year=2025,
        )
        comparison = FileComparison(key=ftp.identity_key(), records=[ftp, s3])
        assert not SyncEngine._s3_is_stale(comparison)

    def test_s3_not_stale_when_origin_size_unknown(self):
        from pysus.management.records import FileComparison, FileRecord

        ftp = FileRecord(
            origin="ftp",
            dataset="SINAN",
            name="DENGBR25.dbc",
            path="ftp/x",
            size=100,
            group="DENG",
            year=2025,
        )
        s3 = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="s3/x",
            size=5000,
            source_size=100,
            group="DENG",
            year=2025,
        )
        comparison = FileComparison(key=ftp.identity_key(), records=[ftp, s3])
        assert not SyncEngine._s3_is_stale(comparison)


class _FakeFTP:
    """Minimal ``ftplib.FTP`` stand-in for liveness/reconnect tests."""

    def __init__(self, fail_noop=False, fail_retr=None):
        self.fail_noop = fail_noop
        self.fail_retr = fail_retr
        self.noop_calls = 0
        self.closed = False
        self.retr_rest = None

    def voidcmd(self, cmd):
        self.noop_calls += 1
        if self.fail_noop:
            raise OSError("stale")
        return "200 OK"

    def size(self, path):
        return 10

    def close(self):
        self.closed = True

    def retrbinary(self, cmd, callback, rest=None):
        self.retr_rest = rest
        if self.fail_retr is not None:
            raise self.fail_retr
        callback(b"bytes")


class _FakeFTPClient:
    """Fake FTP client exposing ``.ftp`` and an async ``connect``."""

    def __init__(self, ftp, next_ftp=None):
        self._ftp = ftp
        self._next = next_ftp
        self.connect_calls = 0

    @property
    def ftp(self):
        return self._ftp

    async def connect(self):
        self.connect_calls += 1
        self._ftp = self._next


class TestLiveFTP:
    @pytest.mark.asyncio
    async def test_connects_when_no_session(self):
        new = _FakeFTP()
        client = _FakeFTPClient(None, next_ftp=new)
        assert await SyncEngine._live_ftp(client) is new
        assert client.connect_calls == 1

    @pytest.mark.asyncio
    async def test_keeps_healthy_session(self):
        ftp = _FakeFTP()
        client = _FakeFTPClient(ftp)
        assert await SyncEngine._live_ftp(client) is ftp
        assert client.connect_calls == 0
        assert ftp.noop_calls == 1

    @pytest.mark.asyncio
    async def test_reconnects_stale_session(self):
        stale = _FakeFTP(fail_noop=True)
        new = _FakeFTP()
        client = _FakeFTPClient(stale, next_ftp=new)
        assert await SyncEngine._live_ftp(client) is new
        assert client.connect_calls == 1
        assert stale.closed is True

    @pytest.mark.asyncio
    async def test_raises_when_connect_yields_no_session(self):
        client = _FakeFTPClient(None, next_ftp=None)
        with pytest.raises(RuntimeError):
            await SyncEngine._live_ftp(client)

    @pytest.mark.asyncio
    async def test_download_once_resets_client_on_failure(self, tmp_path):
        stale = _FakeFTP(fail_retr=OSError("boom"))
        client = _FakeFTPClient(stale)
        file = MagicMock()
        file.client = client
        file.path = "/d/BISP.dbc"
        file.basename = "BISP.dbc"
        file.size = 10
        engine = SyncEngine()
        with pytest.raises(OSError):
            await engine._download_once(file, tmp_path / "out.dbc")
        assert client.ftp is None
        assert stale.closed is True

    @pytest.mark.asyncio
    async def test_download_once_resets_pooled_client_on_failure(
        self, tmp_path
    ):
        pooled = _FakeFTP(fail_retr=OSError("boom"))
        client = _FakeFTPClient(pooled)
        file = MagicMock()
        file.client = client
        file.path = "/d/BISP.dbc"
        file.basename = "BISP.dbc"
        file.size = 10
        engine = SyncEngine()
        with pytest.raises(OSError):
            await engine._download_once(file, tmp_path / "out.dbc", client)
        assert client.ftp is None
        assert pooled.closed is True

    def test_s3_not_stale_when_source_size_zero(self):
        from pysus.management.records import FileComparison, FileRecord

        ftp = FileRecord(
            origin="dadosgov",
            dataset="SINAN",
            name="DENGBR25.csv.zip",
            path="api/x",
            size=0,
            group="DENG",
            year=2025,
        )
        s3 = FileRecord(
            origin="ducklake",
            dataset="SINAN",
            name="DENGBR25.parquet",
            path="s3/x",
            size=5000,
            source_size=100,
            group="DENG",
            year=2025,
        )
        comparison = FileComparison(key=ftp.identity_key(), records=[ftp, s3])
        assert not SyncEngine._s3_is_stale(comparison)
