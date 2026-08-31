"""Phase 3 tests - public origin namespaces.

Covers ``pysus.ftp`` / ``pysus.dadosgov`` / ``pysus.saude``.

Focus areas defined by the roadmap:
- namespaces exist with the correct ``__all__`` (fetchers + discovery);
- bound-wrapper identity is stable between repeated accesses;
- ``pysus.ftp.sinan`` routes to ``client_filter='FTP'`` and ``source='origin'``
  routes to the live origin path;
- discovery (``list_files``) is scoped per origin;
- ``from pysus.ftp import sinan`` style and idempotent re-imports.
"""

from unittest.mock import AsyncMock, patch

import pytest
from pysus.api._impl.source import APPLICABILITY, origin_fetchers
from pysus.api.bag import FileBag
from pysus.api.client import PySUS


@pytest.fixture()
def pysus():
    import pysus  # noqa: F401

    return pysus


def _public_fetchers(*names):
    """Fetch + discovery names always present on every namespace."""
    base = set(names)
    base.update({"list_files", "info", "get_origin_meta"})
    return base


class TestNamespaceExistence:
    def test_registered_on_package(self, pysus):
        for name in ("ftp", "dadosgov", "saude"):
            assert hasattr(pysus, name)
            assert name in pysus.__all__

    def test_all_matches_fetchers_plus_discovery(self, pysus):
        # The public module __all__ equals the platform's fetchers + discovery.
        for ns, canonical in (
            (pysus.ftp, "FTP"),
            (pysus.dadosgov, "DADOSGOV"),
            (pysus.saude, "SAUDE"),
        ):
            fetched = origin_fetchers(canonical)
            expected = _public_fetchers(*fetched)
            assert set(ns.__all__) == expected, canonical

    def test_every_all_name_resolves(self, pysus):
        for ns in (pysus.ftp, pysus.dadosgov, pysus.saude):
            for name in ns.__all__:
                assert callable(getattr(ns, name)), (ns, name)

    def test_all_scoped_to_applicability(self, pysus):
        # A namespace exposes exactly the fetchers applicable to its origin.
        for ns, canonical in (
            (pysus.ftp, "FTP"),
            (pysus.dadosgov, "DADOSGOV"),
            (pysus.saude, "SAUDE"),
        ):
            fetchers = {
                n
                for n in ns.__all__
                if n not in ("list_files", "info", "get_origin_meta")
            }
            assert fetchers == APPLICABILITY[canonical], canonical


class TestBoundIdentity:
    def test_identity_stable_between_access(self, pysus):
        for ns in (pysus.ftp, pysus.dadosgov, pysus.saude):
            for name in ("sinan", "list_files", "info", "get_origin_meta"):
                if not hasattr(ns, name):
                    continue
                assert getattr(ns, name) is getattr(ns, name), (ns, name)

    def test_bound_differs_from_flat(self, pysus):
        # The namespaced fetcher is a distinct wrapper, not the raw flat fn.
        from pysus.api._impl import databases as _db

        assert pysus.ftp.sinan is not _db.sinan
        assert pysus.ftp.sinan.__name__ == "sinan"


class TestIdempotentImports:
    def test_reimport_keeps_working(self, pysus):
        import importlib

        import pysus.ftp as ftp_mod

        for _ in range(2):
            importlib.reload(ftp_mod)
        assert callable(ftp_mod.sinan)
        # package attribute still the module after the reloads
        assert pysus.ftp is ftp_mod

    def test_from_import_after_reload(self, pysus):
        import importlib

        import pysus.dadosgov as dg

        importlib.reload(dg)
        from pysus.dadosgov import sinasc  # noqa: F401

        assert callable(sinasc)


class TestRouting:
    def test_catalog_routes_to_ducklake_client_filter(self, pysus):
        with patch.object(PySUS, "query", new_callable=AsyncMock) as query:
            query.return_value = []
            pysus.ftp.list_files("SINAN", year=2017, state="BR")
            # client filter is bound to FTP
            assert query.call_args.kwargs["client"] == "FTP"
        with patch.object(PySUS, "query", new_callable=AsyncMock) as query:
            query.return_value = []
            pysus.dadosgov.list_files("SINAN", year=2017, state="BR")
            assert query.call_args.kwargs["client"] == "DadosGov"

    def test_source_origin_routes_to_direct(self, pysus):
        with patch(
            "pysus.api._impl.source._fetch_origin_direct",
            new_callable=AsyncMock,
            return_value=[],
        ) as direct:
            pysus.ftp.sinan(disease="deng", year=2017, source="origin")
            direct.assert_awaited_once()

    def test_saude_cnes_routes_to_saude_fetch(self, pysus):
        # Saude exposes its own cnes (CKAN resources), distinct from the
        # FTP/DadosGov monthly-dump cnes, and it routes to _fetch_saude.
        from unittest.mock import AsyncMock, patch

        with patch(
            "pysus.api._impl.databases._fetch_saude",
            new_callable=AsyncMock,
            return_value=["https://example.gov/cnes/estabelecimentos.csv"],
        ) as fetch:
            result = pysus.saude.cnes(download=False)
        fetch.assert_awaited_once()
        assert isinstance(result, FileBag)
        assert result.kind == "remote"
        assert "cnes" in fetch.call_args.kwargs["dataset"]
        # FTP/DadosGov keep their own cnes.
        assert pysus.ftp.cnes is not pysus.saude.cnes


class _StubFile:
    def __init__(self, path):
        self.path = path


class TestDownloadParam:
    def test_download_false_lists_remote_paths(self, pysus):
        files = [
            _StubFile("public/data/ftp/sinan/DENG/2017/_/BR/DENGBR17.parquet")
        ]
        with patch.object(PySUS, "query", new_callable=AsyncMock) as query:
            query.return_value = files
            result = pysus.ftp.sinan(disease="deng", year=2017, download=False)
        assert isinstance(result, FileBag)
        assert result.kind == "remote"
        assert result.paths == [
            "public/data/ftp/sinan/DENG/2017/_/BR/DENGBR17.parquet"
        ]
        query.assert_awaited_once()

    def test_download_false_ignores_as_dataframe(self, pysus):
        files = [_StubFile("public/data/ftp/sinan/a.parquet")]
        with patch.object(PySUS, "query", new_callable=AsyncMock) as query:
            query.return_value = files
            result = pysus.ftp.sinan(
                disease="deng", year=2017, download=False, as_dataframe=True
            )
        # namespaced fetchers always yield a remote FileBag on download=False
        assert isinstance(result, FileBag)
        assert result.kind == "remote"
        assert result.paths == ["public/data/ftp/sinan/a.parquet"]

    def test_download_false_does_not_download(self, pysus):
        files = [_StubFile("public/data/ftp/sinan/a.parquet")]
        with patch.object(PySUS, "query", new_callable=AsyncMock) as query:
            query.return_value = files
            with patch.object(PySUS, "download", new_callable=AsyncMock) as dl:
                pysus.ftp.sinan(disease="deng", year=2017, download=False)
        dl.assert_not_awaited()
