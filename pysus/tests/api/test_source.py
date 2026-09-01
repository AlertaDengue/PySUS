"""Tests for pysus.api._impl.source — the origin/source primitive."""

from unittest.mock import AsyncMock, patch

import pytest
from pysus.api._impl.source import (
    APPLICABILITY,
    ORIGIN_CLIENT_MAP,
    ORIGIN_PREFIXES,
    _client_filter,
    fetch,
    valid_origins,
)
from pysus.api.client import PySUS
from pysus.api.errors import PySUSWarning


class TestOriginConstants:
    def test_valid_origins_exclude_ducklake(self):
        assert valid_origins() == ("FTP", "DADOSGOV", "SAUDE")

    def test_ducklake_not_an_origin(self):
        assert "DUCKLAKE" not in valid_origins()
        assert "DuckLake" not in valid_origins()

    def test_prefixes(self):
        assert ORIGIN_PREFIXES["FTP"] == "public/data/ftp/"
        assert ORIGIN_PREFIXES["DADOSGOV"] == "public/data/dadosgov/"
        assert ORIGIN_PREFIXES["SAUDE"] == "public/data/saude/"

    def test_client_map(self):
        assert ORIGIN_CLIENT_MAP["FTP"] == "ftp"
        assert ORIGIN_CLIENT_MAP["DADOSGOV"] == "dadosgov"
        assert "SAUDE" not in ORIGIN_CLIENT_MAP


class TestClientFilter:
    def test_none(self):
        assert _client_filter(None) is None

    def test_ftp(self):
        assert _client_filter("FTP") == "FTP"

    def test_dadosgov(self):
        assert _client_filter("DADOSGOV") == "DadosGov"

    def test_case_insensitive(self):
        assert _client_filter("ftp") == "FTP"
        assert _client_filter("dadosgov") == "DadosGov"

    def test_ducklake(self):
        assert _client_filter("DUCKLAKE") == "DuckLake"

    def test_unknown(self):
        assert _client_filter("NOPE") is None


class TestApplicability:
    def test_ftp_set(self):
        names = APPLICABILITY["FTP"]
        assert {"sinan", "sinasc", "sim", "sih", "sia", "pni"} <= names
        assert "arboviroses" not in names

    def test_dadosgov_omits_unpublished(self):
        names = APPLICABILITY["DADOSGOV"]
        # CKAN does not publish these → omitted (not exposed-and-405)
        assert {"sinan", "sim", "sinasc", "cnes", "pni", "covid19"} <= names
        assert "sih" not in names
        assert "sia" not in names
        assert "ciha" not in names
        assert "ibge" not in names

    def test_saude_themes(self):
        names = APPLICABILITY["SAUDE"]
        assert {"arboviroses", "vacinacao", "vigilancia_meio_ambiente"} <= names
        assert "sinan" not in names


class TestFetchRouting:
    def test_catalog_default_routes_to_ducklake(self):
        with patch(
            "pysus.api._impl.source._fetch_catalog",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_cat:
            fetch("sinan", year=2020, show_progress=False)
            mock_cat.assert_awaited_once()
            assert mock_cat.call_args.args[1] == "sinan"

    def test_source_origin_default_requires_origin(self):
        from pysus.api.errors import ValidationError

        with pytest.raises(ValidationError):
            fetch("sinan", source="origin", year=2020)

    def test_invalid_source_rejected(self):
        from pysus.api.errors import ValidationError

        with pytest.raises(ValidationError):
            fetch("sinan", source="bogus", year=2020)


class TestOriginNamespaces:
    """Verify the public origin namespace modules."""

    @pytest.fixture()
    def import_pysus(self):
        import pysus  # noqa: F401

        return pysus

    def test_namespaces_registered(self, import_pysus):
        assert hasattr(import_pysus, "ftp")
        assert hasattr(import_pysus, "dadosgov")
        assert hasattr(import_pysus, "saude")

    def test_from_import_style(self):
        from pysus.dadosgov import sinasc
        from pysus.ftp import sinan
        from pysus.saude import arboviroses

        assert sinan.__name__ == "sinan"
        assert sinasc.__name__ == "sinasc"
        assert arboviroses.__name__ == "arboviroses"

    def test_ftp_binds_origin(self):
        import pysus

        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = []
            pysus.ftp.sinan(disease="deng", year=2017, show_progress=False)
            kwargs = mock_fetch.call_args.kwargs
            assert kwargs["origin"] == "FTP"
            assert kwargs["source"] == "catalog"

    def test_saude_binds_origin(self):
        import pysus

        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = []
            pysus.saude.arboviroses(show_progress=False)
            kwargs = mock_fetch.call_args.kwargs
            # Saude flat functions hardcode origin internally
            assert kwargs["origin"] == "Saude"
            assert kwargs["source"] == "catalog"

    def test_rejects_explicit_origin(self):
        import pysus
        from pysus.api.errors import PySUSError

        with pytest.raises(PySUSError):
            pysus.ftp.sinan(disease="deng", year=2017, origin="DadosGov")

    def test_discovery_names_present(self):
        import pysus

        for mod in (pysus.ftp, pysus.dadosgov, pysus.saude):
            for name in ("list_files", "info", "get_origin_meta"):
                assert hasattr(mod, name)

    def test_get_origin_meta(self):
        import pysus

        meta = pysus.ftp.get_origin_meta()
        assert meta["origin"] == "FTP"
        assert "sinan" in meta["fetchers"]

    def test_docstrings_present(self):
        import pysus

        assert pysus.ftp.__doc__
        assert pysus.dadosgov.__doc__
        assert pysus.saude.__doc__
        assert "origin" in pysus.ftp.sinan.__doc__.lower()
        assert pysus.ftp.list_files.__doc__
        assert pysus.saude.info.__doc__
        assert pysus.ftp.get_origin_meta.__doc__

    def test_dadosgov_omits_sih_sia_ciha_ibge(self):
        import pysus

        for name in ("sih", "sia", "ciha", "ibge"):
            assert not hasattr(pysus.dadosgov, name), name


class TestFlatDeprecation:
    """Direct flat calls warn; namespaced calls are silent."""

    def _catalog_empty(self, *args, **kwargs) -> None:
        from unittest.mock import AsyncMock

        with patch(
            "pysus.api._impl.source._fetch_catalog",
            new_callable=AsyncMock,
            return_value=[],
        ):
            pass

    def _flat_warns(self, call):
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            call()
        return [x for x in w if x.category is PySUSWarning]

    def _ns_silent(self, call):
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            call()
        return [x for x in w if x.category is PySUSWarning]

    def test_flat_sinan_warns(self):
        import pysus

        with patch(
            "pysus.api._impl.source._fetch_catalog",
            new_callable=AsyncMock,
            return_value=[],
        ):
            warned = self._flat_warns(
                lambda: pysus.sinan(
                    disease="deng", year=2017, show_progress=False
                )
            )
        assert len(warned) == 1
        assert "deprecated" in str(warned[0].message)
        assert "pysus.ftp.sinan" in str(warned[0].message)

    def test_flat_saude_warns(self):
        import pysus

        with patch(
            "pysus.api._impl.source._fetch_catalog",
            new_callable=AsyncMock,
            return_value=[],
        ):
            warned = self._flat_warns(
                lambda: pysus.arboviroses(show_progress=False)
            )
        assert len(warned) == 1

    def test_namespaced_sinan_silent(self):
        import pysus

        with patch(
            "pysus.api._impl.source._fetch_catalog",
            new_callable=AsyncMock,
            return_value=[],
        ):
            warned = self._ns_silent(
                lambda: pysus.ftp.sinan(
                    disease="deng", year=2017, show_progress=False
                )
            )
        assert warned == []

    def test_namespaced_list_files_silent(self):
        import pysus

        async def _q(**kwargs):
            return []

        with patch.object(PySUS, "query", new=AsyncMock(side_effect=_q)):
            warned = self._ns_silent(
                lambda: pysus.ftp.list_files("SINAN", year=2017, state="BR")
            )
        assert warned == []

    def test_flat_list_files_warns(self):
        import pysus

        async def _q(**kwargs):
            return []

        with patch.object(PySUS, "query", new=AsyncMock(side_effect=_q)):
            warned = self._flat_warns(
                lambda: pysus.list_files("SINAN", year=2017, state="BR")
            )
        assert len(warned) == 1


class TestNamespacedValidation:
    def test_rejects_origin_kwarg(self):
        import pysus
        from pysus.api.errors import PySUSError

        with pytest.raises(PySUSError):
            pysus.ftp.sinan(disease="deng", year=2017, origin="FTP")

    def test_rejects_invalid_source(self):
        import pysus
        from pysus.api.errors import ValidationError

        with pytest.raises(ValidationError):
            pysus.ftp.sinan(disease="deng", year=2017, source="bogus")

    def test_accepts_source_origin(self):
        import pysus

        with patch(
            "pysus.api._impl.source._fetch_origin_direct",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_direct:
            pysus.ftp.sinan(disease="deng", year=2017, source="origin")
            mock_direct.assert_awaited_once()


class TestFetchOriginDirect:
    """Unit coverage for ``_fetch_origin_direct`` (FTP/DadosGov path)."""

    def _make_pysus(self, files):
        from unittest.mock import AsyncMock, MagicMock

        pysus = MagicMock()
        pysus.query = AsyncMock(return_value=files)
        return pysus

    def _run(self, coro):
        import asyncio

        return asyncio.run(coro)

    def _file(self, path):
        from unittest.mock import MagicMock

        f = MagicMock()
        f.path = path
        return f

    def test_unsupported_origin_raises(self):
        from pysus.api._impl.source import _fetch_origin_direct
        from pysus.api.errors import ValidationError

        pysus = self._make_pysus([])
        with pytest.raises(ValidationError):
            self._run(
                _fetch_origin_direct(
                    pysus,
                    "sinan",
                    None,
                    None,
                    2020,
                    None,
                    "BOGUS",
                    None,
                    False,
                    False,
                )
            )

    def test_empty_files_returns_empty_list(self):
        from pysus.api._impl.source import _fetch_origin_direct

        pysus = self._make_pysus([])
        result = self._run(
            _fetch_origin_direct(
                pysus,
                "sinan",
                None,
                None,
                2020,
                None,
                "FTP",
                None,
                False,
                False,
                download=False,
            )
        )
        assert result == []

    def test_empty_files_returns_empty_df_when_as_dataframe(self):
        import pandas as pd
        from pysus.api._impl.source import _fetch_origin_direct

        pysus = self._make_pysus([])
        result = self._run(
            _fetch_origin_direct(
                pysus,
                "sinan",
                None,
                None,
                2020,
                None,
                "FTP",
                None,
                True,
                True,
                download=True,
            )
        )
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_download_false_bag_returns_files(self):
        from pysus.api._impl.source import _fetch_origin_direct

        f = self._file("public/data/ftp/sinan/a.parquet")
        pysus = self._make_pysus([f])
        result = self._run(
            _fetch_origin_direct(
                pysus,
                "sinan",
                None,
                None,
                2020,
                None,
                "FTP",
                None,
                False,
                False,
                download=False,
                _bag=True,
            )
        )
        assert result == [f]

    def test_download_false_returns_paths(self):
        from pysus.api._impl.source import _fetch_origin_direct

        f = self._file("public/data/ftp/sinan/a.parquet")
        pysus = self._make_pysus([f])
        result = self._run(
            _fetch_origin_direct(
                pysus,
                "sinan",
                None,
                None,
                2020,
                None,
                "FTP",
                None,
                False,
                False,
                download=False,
            )
        )
        assert result == ["public/data/ftp/sinan/a.parquet"]

    def test_prefix_filter_keeps_matching(self):
        from pysus.api._impl.source import _fetch_origin_direct

        matching = self._file("public/data/ftp/sinan/a.parquet")
        other = self._file("elsewhere/b.parquet")
        pysus = self._make_pysus([matching, other])
        result = self._run(
            _fetch_origin_direct(
                pysus,
                "sinan",
                None,
                None,
                2020,
                None,
                "FTP",
                None,
                False,
                False,
                download=False,
            )
        )
        assert result == ["public/data/ftp/sinan/a.parquet"]

    def test_download_true_delegates_to_download_files(self):
        import pandas as pd
        from pysus.api._impl import databases as db
        from pysus.api._impl.source import _fetch_origin_direct

        f = self._file("public/data/ftp/sinan/a.parquet")
        pysus = self._make_pysus([f])
        with patch.object(
            db,
            "_download_files",
            new=AsyncMock(
                return_value=pd.DataFrame({"a": [1]}),
            ),
        ) as mock_dl:
            result = self._run(
                _fetch_origin_direct(
                    pysus,
                    "sinan",
                    None,
                    None,
                    2020,
                    None,
                    "FTP",
                    None,
                    False,
                    False,
                )
            )
        mock_dl.assert_awaited_once()
        assert list(result["a"]) == [1]

    def test_saude_origin_delegates(self):
        from pysus.api._impl import databases as db
        from pysus.api._impl.source import _fetch_origin_direct

        pysus = self._make_pysus([])
        with patch.object(
            db,
            "_fetch_saude",
            new=AsyncMock(return_value=["x"]),
        ) as mock_saude:
            result = self._run(
                _fetch_origin_direct(
                    pysus,
                    "arboviroses",
                    None,
                    None,
                    None,
                    None,
                    "SAUDE",
                    None,
                    False,
                    False,
                    download=False,
                )
            )
        mock_saude.assert_awaited_once()
        assert result == ["x"]


class TestCoerceBag:
    def test_none_becomes_empty_filebag(self):
        from pysus.api._impl.source import _coerce_bag
        from pysus.api.bag import FileBag

        result = _coerce_bag(None)
        assert isinstance(result, FileBag)
        assert len(result) == 0

    def test_base_local_file_list_becomes_filebag(self, tmp_path):
        import pandas as pd
        from pysus.api._impl.source import _coerce_bag
        from pysus.api.bag import FileBag
        from pysus.api.client import _run_sync
        from pysus.api.extensions import ExtensionFactory

        p = tmp_path / "a.parquet"
        p.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"a": [1]}).to_parquet(p)
        local = _run_sync(ExtensionFactory.instantiate(p))
        result = _coerce_bag([local])
        assert isinstance(result, FileBag)
        assert result.kind == "local"

    def test_remote_url_strings_become_remote_url_bag(self):
        from pysus.api._impl.source import _coerce_bag
        from pysus.api.bag import FileBag

        result = _coerce_bag(["https://example.com/data.csv"])
        assert isinstance(result, FileBag)
        assert result.kind == "remote"


class TestInstantiateMany:
    def test_skips_uninstantiable_paths(self):
        from unittest.mock import AsyncMock, MagicMock, patch

        from pysus.api._impl.source import _instantiate_many
        from pysus.api.client import _run_sync

        good = MagicMock(name="good")
        with patch(
            "pysus.api.extensions.ExtensionFactory.instantiate",
            new=AsyncMock(side_effect=[OSError("boom"), good]),
        ):
            result = _run_sync(_instantiate_many(["bad", "good"]))
        assert result == [good]


class TestInfoOutput:
    def test_info_prints_table(self, capsys):
        import pysus

        pysus.ftp.info()
        out = capsys.readouterr().out
        assert "SINAN" in out or "sinan" in out
