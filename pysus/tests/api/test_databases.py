import asyncio
import pathlib
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from pysus.api.client import PySUS
from pysus.api.errors import PySUSWarning


class TestSinan:
    def test_sinan_calls_fetch_data(self):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            from pysus.api._impl.databases import sinan

            sinan(disease="dengue", year=2024)
            mock_fetch.assert_called_once()
            args = mock_fetch.call_args
            assert args.kwargs["dataset"] == "sinan"
            assert args.kwargs["group"] == "DENGUE"
            assert args.kwargs["year"] == 2024

    def test_sinan_with_multiple_years(self):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            from pysus.api._impl.databases import sinan

            sinan(disease="dengue", year=[2023, 2024])
            args = mock_fetch.call_args
            assert args.kwargs["year"] == [2023, 2024]


class TestSinasc:
    def test_sinasc_calls_fetch_data(self):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            from pysus.api._impl.databases import sinasc

            sinasc(state="SP", year=2024)
            mock_fetch.assert_called_once()
            args = mock_fetch.call_args
            assert args.kwargs["dataset"] == "sinasc"
            assert args.kwargs["state"] == "SP"
            assert args.kwargs["year"] == 2024

    def test_sinasc_with_group(self):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            from pysus.api._impl.databases import sinasc

            sinasc(state="SP", year=2024, group="DC")
            args = mock_fetch.call_args
            assert args.kwargs["group"] == "DC"


class TestSim:
    def test_sim_calls_fetch_data(self):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            from pysus.api._impl.databases import sim

            sim(state="SP", year=2024)
            mock_fetch.assert_called_once()
            args = mock_fetch.call_args
            assert args.kwargs["dataset"] == "sim"
            assert args.kwargs["state"] == "SP"


class TestSih:
    def test_sih_calls_fetch_data(self):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            from pysus.api._impl.databases import sih

            sih(state="SP", year=2024, month=1)
            mock_fetch.assert_called_once()
            args = mock_fetch.call_args
            assert args.kwargs["dataset"] == "sih"
            assert args.kwargs["state"] == "SP"
            assert args.kwargs["month"] == 1

    def test_sih_with_multiple_months(self):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            from pysus.api._impl.databases import sih

            sih(state="SP", year=2024, month=[1, 2, 3])
            args = mock_fetch.call_args
            assert args.kwargs["month"] == [1, 2, 3]


class TestSia:
    def test_sia_calls_fetch_data(self):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            from pysus.api._impl.databases import sia

            sia(state="SP", year=2024, month=1)
            mock_fetch.assert_called_once()
            args = mock_fetch.call_args
            assert args.kwargs["dataset"] == "sia"


class TestPni:
    def test_pni_calls_fetch_data(self):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            from pysus.api._impl.databases import pni

            pni(state="SP", year=2024)
            mock_fetch.assert_called_once()
            args = mock_fetch.call_args
            assert args.kwargs["dataset"] == "pni"


class TestIbge:
    def test_ibge_calls_fetch_data(self):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            from pysus.api._impl.databases import ibge

            ibge(year=2024)
            mock_fetch.assert_called_once()
            args = mock_fetch.call_args
            assert args.kwargs["dataset"] == "ibge"

    def test_ibge_with_group(self):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            from pysus.api._impl.databases import ibge

            ibge(year=2024, group="IBGE")
            args = mock_fetch.call_args
            assert args.kwargs["group"] == "IBGE"


class TestCnes:
    def test_cnes_calls_fetch_data(self):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            from pysus.api._impl.databases import cnes

            cnes(state="SP", year=2024, month=1)
            mock_fetch.assert_called_once()
            args = mock_fetch.call_args
            assert args.kwargs["dataset"] == "cnes"


class TestCiha:
    def test_ciha_calls_fetch_data(self):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            from pysus.api._impl.databases import ciha

            ciha(state="SP", year=2024, month=1)
            mock_fetch.assert_called_once()
            args = mock_fetch.call_args
            assert args.kwargs["dataset"] == "ciha"
            assert args.kwargs["group"] == "CIHA"


class TestFetchData:
    def test_fetch_data_single_year(self):
        with patch("pysus.api.client.PySUS") as mock_pysus_class:
            mock_pysus = MagicMock()
            enter_mock = AsyncMock(return_value=mock_pysus)
            exit_mock = AsyncMock()
            mock_pysus_class.return_value.__aenter__ = enter_mock
            mock_pysus_class.return_value.__aexit__ = exit_mock

            mock_file = MagicMock()
            mock_file.path = "/tmp/test.parquet"
            mock_pysus.query = AsyncMock(return_value=[mock_file])
            mock_pysus.download = AsyncMock(return_value=mock_file)
            mock_pysus.read_parquet.return_value.df.return_value = MagicMock()

            from pysus.api._impl.databases import _fetch_data

            _fetch_data(dataset="sinan", year=2024, show_progress=False)

            mock_pysus.query.assert_called_once_with(
                client=None,
                dataset="sinan",
                group=None,
                state=None,
                year=2024,
                month=None,
            )

    def test_fetch_data_multiple_years(self):
        with patch("pysus.api.client.PySUS") as mock_pysus_class:
            mock_pysus = MagicMock()
            enter_mock = AsyncMock(return_value=mock_pysus)
            exit_mock = AsyncMock()
            mock_pysus_class.return_value.__aenter__ = enter_mock
            mock_pysus_class.return_value.__aexit__ = exit_mock

            mock_file = MagicMock()
            mock_file.path = "/tmp/test.parquet"
            mock_pysus.query = AsyncMock(return_value=[mock_file])
            mock_pysus.download = AsyncMock(return_value=mock_file)
            mock_pysus.read_parquet.return_value.df.return_value = MagicMock()

            from pysus.api._impl.databases import _fetch_data

            years = [2023, 2024]
            _fetch_data(dataset="sinan", year=years, show_progress=False)

            assert mock_pysus.query.call_count == 1

    def test_fetch_data_with_group_filter(self):
        with patch("pysus.api.client.PySUS") as mock_pysus_class:
            mock_pysus = MagicMock()
            enter_mock = AsyncMock(return_value=mock_pysus)
            exit_mock = AsyncMock()
            mock_pysus_class.return_value.__aenter__ = enter_mock
            mock_pysus_class.return_value.__aexit__ = exit_mock

            mock_file = MagicMock()
            mock_file.path = "/tmp/test.parquet"
            mock_pysus.query = AsyncMock(return_value=[mock_file])
            mock_pysus.download = AsyncMock(return_value=mock_file)
            mock_pysus.read_parquet.return_value.df.return_value = MagicMock()

            from pysus.api._impl.databases import _fetch_data

            _fetch_data(
                dataset="sinan",
                group="DENGUE",
                state="SP",
                show_progress=False,
            )

            mock_pysus.query.assert_called_once_with(
                client=None,
                dataset="sinan",
                group="DENGUE",
                state="SP",
                year=None,
                month=None,
            )

    def test_fetch_data_empty_result(self):
        with patch("pysus.api.client.PySUS") as mock_pysus_class:
            mock_pysus = MagicMock()
            enter_mock = AsyncMock(return_value=mock_pysus)
            exit_mock = AsyncMock()
            mock_pysus_class.return_value.__aenter__ = enter_mock
            mock_pysus_class.return_value.__aexit__ = exit_mock

            mock_pysus.query = AsyncMock(return_value=[])

            import pandas as pd
            from pysus.api._impl.databases import _fetch_data

            result = _fetch_data(
                dataset="sinan",
                year=2024,
                show_progress=False,
                as_dataframe=True,
            )

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0

    def test_fetch_data_without_progress(self):
        with patch("pysus.api.client.PySUS") as mock_pysus_class:
            mock_pysus = MagicMock()
            enter_mock = AsyncMock(return_value=mock_pysus)
            exit_mock = AsyncMock()
            mock_pysus_class.return_value.__aenter__ = enter_mock
            mock_pysus_class.return_value.__aexit__ = exit_mock

            mock_file = MagicMock()
            mock_file.path = "/tmp/test.parquet"
            mock_pysus.query = AsyncMock(return_value=[mock_file])
            mock_pysus.download = AsyncMock(return_value=mock_file)
            mock_pysus.read_parquet.return_value.df.return_value = MagicMock()

            from pysus.api._impl.databases import _fetch_data

            _fetch_data(
                dataset="sinan",
                year=2024,
                show_progress=False,
            )

            mock_pysus.download.assert_called_once()

    def test_fetch_data_no_files(self):
        with patch("pysus.api.client.PySUS") as mock_pysus_class:
            mock_pysus = MagicMock()
            enter_mock = AsyncMock(return_value=mock_pysus)
            exit_mock = AsyncMock()
            mock_pysus_class.return_value.__aenter__ = enter_mock
            mock_pysus_class.return_value.__aexit__ = exit_mock

            mock_pysus.query = AsyncMock(return_value=[])

            import pandas as pd
            from pysus.api._impl.databases import _fetch_data

            result = _fetch_data(
                dataset="sinan",
                year=2024,
                show_progress=True,
                as_dataframe=True,
            )

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
            mock_pysus.download.assert_not_called()

    def test_fetch_data_with_progress(self):
        with (
            patch("pysus.api.client.PySUS") as mock_pysus_class,
            patch(
                "pysus.api._impl.databases.tqdm.gather",
                new_callable=AsyncMock,
                return_value=[MagicMock(), MagicMock()],
            ) as mock_tqdm_gather,
        ):
            mock_pysus = MagicMock()
            enter_mock = AsyncMock(return_value=mock_pysus)
            exit_mock = AsyncMock()
            mock_pysus_class.return_value.__aenter__ = enter_mock
            mock_pysus_class.return_value.__aexit__ = exit_mock

            mock_file = MagicMock()
            mock_file.path = "/tmp/test.parquet"
            mock_pysus.query = AsyncMock(return_value=[mock_file, mock_file])
            mock_pysus.download = AsyncMock(return_value=mock_file)
            mock_pysus.read_parquet.return_value.df.return_value = MagicMock()

            from pysus.api._impl.databases import _fetch_data

            _fetch_data(dataset="sinan", year=2024, show_progress=True)

            assert mock_tqdm_gather.called

            called_args = mock_tqdm_gather.call_args[0]
            assert len(called_args) == 2


class TestFetchDataRunningLoop:
    def test_fetch_data_running_loop_no_nest_asyncio_raises(self):
        saved = sys.modules.pop("nest_asyncio", None)
        import builtins

        real_import = builtins.__import__

        def raising_import(name, *args, **kwargs):
            if name == "nest_asyncio":
                raise ImportError(f"No module named {name}")
            return real_import(name, *args, **kwargs)

        try:

            async def _inner():
                from pysus.api._impl.databases import _fetch_data

                with patch("builtins.__import__", side_effect=raising_import):
                    with pytest.raises(
                        RuntimeError, match="nest_asyncio is required"
                    ):
                        _fetch_data(
                            dataset="sinan",
                            year=2024,
                            show_progress=False,
                        )

            asyncio.run(_inner())
        finally:
            if saved is not None:
                sys.modules["nest_asyncio"] = saved

    def test_fetch_data_running_loop_with_nest_asyncio(self):
        nest_mock = MagicMock()

        async def _inner():
            with (
                patch("pysus.api.client.PySUS") as mock_pysus_class,
                patch.dict("sys.modules", {"nest_asyncio": nest_mock}),
            ):
                mock_pysus = MagicMock()
                mock_pysus_class.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus
                )
                mock_pysus_class.return_value.__aexit__ = AsyncMock()
                mock_pysus.query = AsyncMock(return_value=[])

                from pysus.api._impl.databases import _fetch_data

                loop = asyncio.get_running_loop()
                expected = MagicMock()

                with patch.object(
                    loop, "run_until_complete", return_value=expected
                ):
                    result = _fetch_data(
                        dataset="sinan",
                        year=2024,
                        show_progress=False,
                    )
                    nest_mock.apply.assert_called_once()
                    assert result == expected

        asyncio.run(_inner())


class TestListFiles:
    def _mock_asyncio_run(self, return_value):
        import asyncio

        def _run(coro):
            if asyncio.iscoroutine(coro):
                coro.close()
            return return_value

        return _run

    def test_list_files_returns_dataframe(self):
        import pandas as pd

        ret = pd.DataFrame(
            {"name": ["test.parquet"], "path": ["/test.parquet"]}
        )

        with patch(
            "pysus.api._impl.databases.asyncio.run",
            side_effect=self._mock_asyncio_run(ret),
        ):
            from pysus.api._impl.databases import list_files

            result = list_files(dataset="SINAN")

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1

    def test_list_files_with_filters(self):
        import pandas as pd

        ret = pd.DataFrame(
            {
                "name": ["test1.parquet", "test2.parquet"],
                "path": ["/test1.parquet", "/test2.parquet"],
                "dataset": ["sinan", "sinan"],
                "year": [2024, 2023],
                "month": [1, 2],
                "state": ["SP", "RJ"],
                "modify": ["2024-01-01", "2024-01-02"],
            }
        )

        with patch(
            "pysus.api._impl.databases.asyncio.run",
            side_effect=self._mock_asyncio_run(ret),
        ):
            from pysus.api._impl.databases import list_files

            result = list_files(
                dataset="SINAN",
                group="DENGUE",
                state="SP",
                year=2024,
                month=1,
            )

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert "name" in result.columns
            assert "path" in result.columns
            assert "dataset" in result.columns
            assert "year" in result.columns
            assert "month" in result.columns
            assert "state" in result.columns
            assert "modify" in result.columns

    def test_list_files_empty_result(self):
        import pandas as pd

        ret = pd.DataFrame()

        with patch(
            "pysus.api._impl.databases.asyncio.run",
            side_effect=self._mock_asyncio_run(ret),
        ):
            from pysus.api._impl.databases import list_files

            result = list_files(dataset="SINAN")

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0

    def test_list_files_with_real_coroutine(self):
        import pandas as pd

        mock_record = MagicMock()
        mock_record.path = "/remote/sinan/dengue.parquet"
        mock_record.dataset.name = "sinan"
        mock_record.group.name = "DENGUE"
        mock_record.record.year = 2024
        mock_record.record.month = 1
        mock_record.record.state = "SP"
        mock_record.record.origin_modified = "2024-01-15"

        with patch("pysus.api.client.PySUS") as mock_pysus_class:
            mock_pysus = MagicMock()
            mock_pysus_class.return_value.__aenter__ = AsyncMock(
                return_value=mock_pysus
            )
            mock_pysus_class.return_value.__aexit__ = AsyncMock()
            mock_pysus.query = AsyncMock(return_value=[mock_record])

            from pysus.api._impl.databases import list_files

            result = list_files(dataset="SINAN", year=2024, month=1)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert result.iloc[0]["name"] == "dengue.parquet"
            assert result.iloc[0]["path"] == "/remote/sinan/dengue.parquet"
            assert result.iloc[0]["dataset"] == "sinan"
            assert result.iloc[0]["group"] == "DENGUE"
            assert result.iloc[0]["year"] == 2024
            assert result.iloc[0]["month"] == 1
            assert result.iloc[0]["state"] == "SP"
            assert result.iloc[0]["modify"] == "2024-01-15"

    def test_list_files_with_none_fields(self):
        mock_record = MagicMock()
        mock_record.path = "/remote/sinan/dengue.parquet"
        mock_record.dataset = None
        mock_record.group = None
        mock_record.record.year = 2024
        mock_record.record.month = 1
        mock_record.record.state = "SP"
        mock_record.record.origin_modified = "2024-01-15"

        with patch("pysus.api.client.PySUS") as mock_pysus_class:
            mock_pysus = MagicMock()
            mock_pysus_class.return_value.__aenter__ = AsyncMock(
                return_value=mock_pysus
            )
            mock_pysus_class.return_value.__aexit__ = AsyncMock()
            mock_pysus.query = AsyncMock(return_value=[mock_record])

            from pysus.api._impl.databases import list_files

            result = list_files(dataset="SINAN")

            assert result.iloc[0]["dataset"] is None
            assert result.iloc[0]["group"] is None

    def test_list_files_with_multiple_records(self):
        records = []
        for i in range(3):
            r = MagicMock()
            r.path = f"/remote/sinan/file{i}.parquet"
            r.dataset.name = "sinan"
            r.group.name = "DENGUE"
            r.record.year = 2024
            r.record.month = i + 1
            r.record.state = "SP"
            r.record.origin_modified = "2024-01-15"
            records.append(r)

        with patch("pysus.api.client.PySUS") as mock_pysus_class:
            mock_pysus = MagicMock()
            mock_pysus_class.return_value.__aenter__ = AsyncMock(
                return_value=mock_pysus
            )
            mock_pysus_class.return_value.__aexit__ = AsyncMock()
            mock_pysus.query = AsyncMock(
                side_effect=[records[:2], records[2:]],
            )

            from pysus.api._impl.databases import list_files

            result = list_files(dataset="SINAN", year=[2023, 2024])

            assert len(result) == 3
            assert mock_pysus.query.call_count == 2


class TestCovid19:
    def test_covid19_calls_fetch_data(self):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            from pysus.api._impl.databases import covid19

            covid19()
            mock_fetch.assert_called_once()
            args = mock_fetch.call_args
            assert args.kwargs["dataset"] == "covid19"


class TestSaudeConvenienceFunctions:
    """Tests for the 18 Saude-portal convenience functions."""

    SAUDE_FUNCTIONS = [
        ("arboviroses", "arboviroses"),
        ("assistencia_saude", "assistencia_saude"),
        ("atencao_primaria", "atencao_primaria"),
        ("bnafar", "bnafar"),
        ("ciencia_tecnologia", "ciencia_tecnologia"),
        ("diagnosticos_tratamentos", "diagnosticos_tratamentos"),
        ("economia_saude", "economia_saude"),
        ("educacao_saude", "educacao_saude"),
        ("macro_saude", "macro_saude"),
        ("ouvidoria", "ouvidoria"),
        ("outros_temas", "outros_temas"),
        ("pda", "pda"),
        ("prevencao_promocao", "prevencao_promocao"),
        ("sisagua", "sisagua"),
        ("sisvan", "sisvan"),
        ("saude_indigena", "saude_indigena"),
        ("vacinacao", "vacinacao"),
        ("vigilancia_meio_ambiente", "vigilancia_meio_ambiente"),
    ]

    @pytest.mark.parametrize("func_name,dataset", SAUDE_FUNCTIONS)
    def test_calls_fetch_data_with_saude_origin(self, func_name, dataset):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            import pysus.api._impl.databases as mod

            fn = getattr(mod, func_name)
            fn()
            mock_fetch.assert_called_once()
            args = mock_fetch.call_args
            assert args.kwargs["dataset"] == dataset
            assert args.kwargs["origin"] == "Saude"

    @pytest.mark.parametrize("func_name,dataset", SAUDE_FUNCTIONS)
    def test_passes_kwargs_through(self, func_name, dataset):
        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            import pysus.api._impl.databases as mod

            fn = getattr(mod, func_name)
            fn(as_dataframe=True, columns=["A", "B"])
            args = mock_fetch.call_args
            assert args.kwargs["as_dataframe"] is True
            assert args.kwargs["columns"] == ["A", "B"]


class TestFetchDataSaudeRouting:
    """Verify _fetch_data routes to _fetch_saude for Saude origin."""

    def test_saude_origin_routes_to_fetch_saude(self):
        with patch(
            "pysus.api._impl.databases._fetch_saude",
            new_callable=AsyncMock,
            return_value=pd.DataFrame({"x": [1]}),
        ) as mock_saude:
            from pysus.api._impl.databases import _fetch_data

            result = _fetch_data(
                dataset="arboviroses",
                origin="Saude",
                as_dataframe=True,
            )
            mock_saude.assert_called_once()
            assert isinstance(result, pd.DataFrame)

    def test_non_saude_origin_routes_to_fetch_ducklake(self):
        with patch(
            "pysus.api._impl.databases._fetch_ducklake",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_dl:
            from pysus.api._impl.databases import _fetch_data

            _fetch_data(dataset="sinan", year=2024)
            mock_dl.assert_called_once()

    def test_none_origin_routes_to_fetch_ducklake(self):
        with patch(
            "pysus.api._impl.databases._fetch_ducklake",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_dl:
            from pysus.api._impl.databases import _fetch_data

            _fetch_data(dataset="sinan", year=2024)
            mock_dl.assert_called_once()


class TestFetchSaude:
    """Tests for the _fetch_saude async helper."""

    def test_empty_entries_returns_empty_df(self):
        async def _run():
            with patch("pysus.api.client.PySUS") as mock_cls:
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()
                saude_mock = AsyncMock()
                saude_mock.list_datasets = AsyncMock(return_value=[])
                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="arboviroses",
                    as_dataframe=True,
                )
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 0

        asyncio.run(_run())

    def test_empty_entries_returns_empty_list(self):
        async def _run():
            with patch("pysus.api.client.PySUS") as mock_cls:
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()
                saude_mock = AsyncMock()
                saude_mock.list_datasets = AsyncMock(return_value=[])
                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="arboviroses",
                    as_dataframe=False,
                )
                assert result == []

        asyncio.run(_run())

    def test_downloads_csv_resources(self):
        async def _run():
            with (
                patch("pysus.api.client.PySUS") as mock_cls,
                patch.object(pathlib.Path, "mkdir"),
            ):
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                saude_mock = AsyncMock()
                entry = MagicMock()
                entry.name = "dengue-2024"
                saude_mock.list_datasets = AsyncMock(return_value=[entry])

                resource = MagicMock()
                resource.id = "res1"
                resource.url = "https://example.com/data.csv"
                pkg = MagicMock()
                pkg.resources = [resource]
                saude_mock.fetch_dataset = AsyncMock(return_value=pkg)
                saude_mock.download_resource = AsyncMock(
                    return_value=Path("/tmp/test.csv"),
                )

                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="arboviroses",
                    show_progress=False,
                )
                assert len(result) == 1
                saude_mock.download_resource.assert_called_once()

        asyncio.run(_run())

    def test_skips_non_csv_resources(self):
        async def _run():
            with (
                patch("pysus.api.client.PySUS") as mock_cls,
                patch.object(pathlib.Path, "mkdir"),
            ):
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                saude_mock = AsyncMock()
                entry = MagicMock()
                entry.name = "dataset-x"
                saude_mock.list_datasets = AsyncMock(return_value=[entry])

                resource = MagicMock()
                resource.id = "res1"
                resource.url = "https://example.com/data.pdf"
                pkg = MagicMock()
                pkg.resources = [resource]
                saude_mock.fetch_dataset = AsyncMock(return_value=pkg)
                saude_mock.download_resource = AsyncMock()

                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="arboviroses",
                    show_progress=False,
                )
                assert result == []
                saude_mock.download_resource.assert_not_called()

        asyncio.run(_run())

    def test_exception_in_fetch_dataset_continues(self):
        async def _run():
            with (
                patch("pysus.api.client.PySUS") as mock_cls,
                patch.object(pathlib.Path, "mkdir"),
            ):
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                saude_mock = AsyncMock()
                entry1 = MagicMock()
                entry1.name = "bad-dataset"
                entry2 = MagicMock()
                entry2.name = "good-dataset"
                saude_mock.list_datasets = AsyncMock(
                    return_value=[entry1, entry2],
                )

                resource = MagicMock()
                resource.id = "res1"
                resource.url = "https://example.com/data.csv"
                pkg = MagicMock()
                pkg.resources = [resource]

                saude_mock.fetch_dataset = AsyncMock(
                    side_effect=[RuntimeError("boom"), pkg],
                )
                saude_mock.download_resource = AsyncMock(
                    return_value=Path("/tmp/test.csv"),
                )

                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="arboviroses",
                    show_progress=False,
                )
                assert len(result) == 1

        asyncio.run(_run())

    def test_as_dataframe_reads_csv_files(self):
        import tempfile

        async def _run():
            with (
                patch("pysus.api.client.PySUS") as mock_cls,
                patch.object(pathlib.Path, "mkdir"),
            ):
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                with tempfile.NamedTemporaryFile(
                    mode="w",
                    suffix=".csv",
                    delete=False,
                ) as f:
                    f.write("col_a,col_b\n1,2\n3,4\n")
                    tmp_path = Path(f.name)

                saude_mock = AsyncMock()
                entry = MagicMock()
                entry.name = "dataset-y"
                saude_mock.list_datasets = AsyncMock(return_value=[entry])

                resource = MagicMock()
                resource.id = "res1"
                resource.url = "https://example.com/data.csv"
                pkg = MagicMock()
                pkg.resources = [resource]
                saude_mock.fetch_dataset = AsyncMock(return_value=pkg)
                saude_mock.download_resource = AsyncMock(
                    return_value=tmp_path,
                )

                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="arboviroses",
                    as_dataframe=True,
                    show_progress=False,
                )
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 2
                assert "col_a" in result.columns

                tmp_path.unlink(missing_ok=True)

        asyncio.run(_run())

    def test_column_filter_in_dataframe(self):
        import tempfile

        async def _run():
            with (
                patch("pysus.api.client.PySUS") as mock_cls,
                patch.object(pathlib.Path, "mkdir"),
            ):
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                with tempfile.NamedTemporaryFile(
                    mode="w",
                    suffix=".csv",
                    delete=False,
                ) as f:
                    f.write("col_a,col_b\n1,2\n")
                    tmp_path = Path(f.name)

                saude_mock = AsyncMock()
                entry = MagicMock()
                entry.name = "ds-z"
                saude_mock.list_datasets = AsyncMock(return_value=[entry])

                resource = MagicMock()
                resource.id = "r1"
                resource.url = "https://example.com/data.csv"
                pkg = MagicMock()
                pkg.resources = [resource]
                saude_mock.fetch_dataset = AsyncMock(return_value=pkg)
                saude_mock.download_resource = AsyncMock(
                    return_value=tmp_path,
                )

                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="arboviroses",
                    as_dataframe=True,
                    columns=["col_a"],
                    show_progress=False,
                )
                assert list(result.columns) == ["col_a"]

                tmp_path.unlink(missing_ok=True)

        asyncio.run(_run())

    def test_slug_only_theme_resolves_via_catalog(self):
        """CNES/SISVAN (no CKAN group, only slug_patterns) list the catalog."""

        async def _run():
            with patch("pysus.api.client.PySUS") as mock_cls:
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                saude_mock = AsyncMock()
                matches = MagicMock()
                matches.name = "cnes-estabelecimentos"
                non = MagicMock()
                non.name = "some-other-dataset"

                async def _gen():
                    for e in (matches, non):
                        yield e

                saude_mock.iter_datasets = _gen
                saude_mock.download_resource = AsyncMock()

                resource = MagicMock()
                resource.id = "r1"
                resource.url = "https://example.com/cnes.csv"
                pkg = MagicMock()
                pkg.resources = [resource]
                saude_mock.fetch_dataset = AsyncMock(return_value=pkg)

                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="cnes",
                    show_progress=False,
                )
                assert len(result) == 1
                saude_mock.fetch_dataset.assert_awaited_once_with(
                    "cnes-estabelecimentos"
                )

        asyncio.run(_run())

    def test_zipped_csv_resource_is_captured_by_format(self):
        """Resources stored as ``*_csv.zip`` (format=CSV) are included."""

        async def _run():
            with (
                patch("pysus.api.client.PySUS") as mock_cls,
                patch.object(pathlib.Path, "mkdir"),
            ):
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                saude_mock = AsyncMock()
                entry = MagicMock()
                entry.name = "esavi"
                saude_mock.list_datasets = AsyncMock(return_value=[entry])

                zip_res = MagicMock()
                zip_res.id = "zip1"
                zip_res.url = "https://example.com/Esavi_csv.zip"
                zip_res.format = "CSV"
                pdf_res = MagicMock()
                pdf_res.id = "pdf1"
                pdf_res.url = "https://example.com/manual.pdf"
                pdf_res.format = "PDF"
                placeholder = MagicMock()
                placeholder.id = "ph1"
                placeholder.url = "."
                placeholder.format = "CSV"
                pkg = MagicMock()
                pkg.resources = [zip_res, pdf_res, placeholder]
                saude_mock.fetch_dataset = AsyncMock(return_value=pkg)
                saude_mock.download_resource = AsyncMock(
                    return_value=Path("/tmp/Esavi_csv.zip"),
                )

                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="vacinacao",
                    show_progress=False,
                )
                assert len(result) == 1
                saude_mock.download_resource.assert_awaited_once_with(
                    "esavi",
                    resource_id="zip1",
                    dest_dir=Path("/tmp/test_cache")
                    / "downloads"
                    / "saude"
                    / "vacinacao",
                )

        asyncio.run(_run())

    def test_download_false_lists_urls_only(self):
        """download=False returns CSV URLs without downloading."""

        async def _run():
            with patch("pysus.api.client.PySUS") as mock_cls:
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                saude_mock = AsyncMock()
                entry = MagicMock()
                entry.name = "esavi"
                saude_mock.list_datasets = AsyncMock(return_value=[entry])

                csv_res = MagicMock()
                csv_res.id = "csv1"
                csv_res.url = "https://example.com/Esavi_csv.zip"
                csv_res.format = "CSV"
                pkg = MagicMock()
                pkg.resources = [csv_res]
                saude_mock.fetch_dataset = AsyncMock(return_value=pkg)
                saude_mock.download_resource = AsyncMock()

                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="vacinacao",
                    download=False,
                    show_progress=False,
                )
                assert result == ["https://example.com/Esavi_csv.zip"]
                saude_mock.download_resource.assert_not_called()

        asyncio.run(_run())

    def test_all_fetch_dataset_fail_returns_empty_df(self):
        async def _run():
            with patch("pysus.api.client.PySUS") as mock_cls:
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                saude_mock = AsyncMock()
                entry = MagicMock()
                entry.name = "bad"
                saude_mock.list_datasets = AsyncMock(return_value=[entry])
                saude_mock.fetch_dataset = AsyncMock(
                    side_effect=RuntimeError("boom"),
                )

                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="arboviroses",
                    as_dataframe=True,
                    show_progress=False,
                )
                assert isinstance(result, pd.DataFrame)
                assert result.empty

        asyncio.run(_run())

    def test_all_downloads_fail_returns_empty_list(self):
        async def _run():
            with (
                patch("pysus.api.client.PySUS") as mock_cls,
                patch.object(pathlib.Path, "mkdir"),
            ):
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                saude_mock = AsyncMock()
                entry = MagicMock()
                entry.name = "esavi"
                saude_mock.list_datasets = AsyncMock(return_value=[entry])

                resource = MagicMock()
                resource.id = "r1"
                resource.url = "https://example.com/data.csv"
                pkg = MagicMock()
                pkg.resources = [resource]
                saude_mock.fetch_dataset = AsyncMock(return_value=pkg)
                saude_mock.download_resource = AsyncMock(
                    side_effect=RuntimeError("net down"),
                )

                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="arboviroses",
                    show_progress=False,
                )
                assert result == []
                assert saude_mock.download_resource.await_count == 1

        asyncio.run(_run())

    def test_all_downloads_fail_as_dataframe_returns_empty_df(self):
        async def _run():
            with (
                patch("pysus.api.client.PySUS") as mock_cls,
                patch.object(pathlib.Path, "mkdir"),
            ):
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                saude_mock = AsyncMock()
                entry = MagicMock()
                entry.name = "esavi"
                saude_mock.list_datasets = AsyncMock(return_value=[entry])

                resource = MagicMock()
                resource.id = "r1"
                resource.url = "https://example.com/data.csv"
                pkg = MagicMock()
                pkg.resources = [resource]
                saude_mock.fetch_dataset = AsyncMock(return_value=pkg)
                saude_mock.download_resource = AsyncMock(
                    side_effect=RuntimeError("net down"),
                )

                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="arboviroses",
                    as_dataframe=True,
                    show_progress=False,
                )
                assert isinstance(result, pd.DataFrame)
                assert result.empty

        asyncio.run(_run())

    def test_unreadable_downloads_yield_empty_df(self):
        async def _run():
            with (
                patch("pysus.api.client.PySUS") as mock_cls,
                patch.object(pathlib.Path, "mkdir"),
            ):
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                saude_mock = AsyncMock()
                entry = MagicMock()
                entry.name = "esavi"
                saude_mock.list_datasets = AsyncMock(return_value=[entry])

                resource = MagicMock()
                resource.id = "r1"
                resource.url = "https://example.com/data.csv"
                pkg = MagicMock()
                pkg.resources = [resource]
                saude_mock.fetch_dataset = AsyncMock(return_value=pkg)
                # Downloaded file does not exist -> _saude_csv_to_frame -> None.
                saude_mock.download_resource = AsyncMock(
                    return_value=Path("/tmp/nao-existe.csv"),
                )

                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="arboviroses",
                    as_dataframe=True,
                    show_progress=False,
                )
                assert isinstance(result, pd.DataFrame)
                assert result.empty

        asyncio.run(_run())

    def test_group_backed_spec_filters_by_slug_pattern(self):
        async def _run():
            with patch("pysus.api.client.PySUS") as mock_cls:
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                saude_mock = AsyncMock()
                match = MagicMock()
                match.name = "sisagua-2024"
                skip = MagicMock()
                skip.name = "outro-tema"
                saude_mock.list_datasets = AsyncMock(
                    return_value=[match, skip],
                )
                saude_mock.download_resource = AsyncMock()

                resource = MagicMock()
                resource.id = "r1"
                resource.url = "https://example.com/sisagua.csv"
                pkg = MagicMock()
                pkg.resources = [resource]
                saude_mock.fetch_dataset = AsyncMock(return_value=pkg)

                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="sisagua",
                    download=False,
                )
                assert result == ["https://example.com/sisagua.csv"]
                saude_mock.fetch_dataset.assert_awaited_once_with(
                    "sisagua-2024"
                )

        asyncio.run(_run())

    def test_unknown_theme_falls_back_to_group_map(self):
        """Legacy names (no spec) still resolve through _SAUDE_GROUP_MAP."""

        async def _run():
            with patch("pysus.api.client.PySUS") as mock_cls:
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                saude_mock = AsyncMock()
                entry = MagicMock()
                entry.name = "vig-tema"
                saude_mock.list_datasets = AsyncMock(return_value=[entry])
                saude_mock.download_resource = AsyncMock()

                resource = MagicMock()
                resource.id = "r1"
                resource.url = "https://example.com/data.csv"
                pkg = MagicMock()
                pkg.resources = [resource]
                saude_mock.fetch_dataset = AsyncMock(return_value=pkg)

                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="vigilancia_meio_ambiente",
                    download=False,
                )
                assert result == ["https://example.com/data.csv"]
                saude_mock.list_datasets.assert_awaited_once_with(
                    group="vigilancia-e-meio-ambiente"
                )

        asyncio.run(_run())

    def test_show_progress_true_downloads(self):
        async def _run():
            with (
                patch("pysus.api.client.PySUS") as mock_cls,
                patch.object(pathlib.Path, "mkdir"),
            ):
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                saude_mock = AsyncMock()
                entry = MagicMock()
                entry.name = "esavi"
                saude_mock.list_datasets = AsyncMock(return_value=[entry])

                resource = MagicMock()
                resource.id = "r1"
                resource.url = "https://example.com/data.csv"
                pkg = MagicMock()
                pkg.resources = [resource]
                saude_mock.fetch_dataset = AsyncMock(return_value=pkg)
                saude_mock.download_resource = AsyncMock(
                    return_value=Path("/tmp/data.csv"),
                )

                mock_pysus.get_saude = AsyncMock(return_value=saude_mock)
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_saude

                result = await _fetch_saude(
                    dataset="arboviroses",
                    show_progress=True,
                )
                assert len(result) == 1

        asyncio.run(_run())


class TestDownloadFiles:

    def test_empty_files_returns_empty_list(self):
        async def _run():
            from pysus.api._impl.databases import _download_files

            mock_pysus = MagicMock()
            result = await _download_files(mock_pysus, [])
            assert result == []

        asyncio.run(_run())

    def test_empty_files_returns_empty_df_when_as_dataframe(self):
        async def _run():
            from pysus.api._impl.databases import _download_files

            mock_pysus = MagicMock()
            result = await _download_files(mock_pysus, [], as_dataframe=True)
            assert isinstance(result, pd.DataFrame)
            assert result.empty

        asyncio.run(_run())

    def test_columns_filter_applied_to_dataframe(self):
        async def _run():
            from pysus.api._impl.databases import _download_files

            mock_pysus = MagicMock()
            mock_pysus.download = AsyncMock(
                side_effect=lambda f: f,
            )
            f1 = MagicMock()
            f1.path = "a.parquet"
            df = pd.DataFrame({"x": [1], "y": [2], "z": [3]})
            mock_pysus.read_parquet = MagicMock(return_value=df)

            result = await _download_files(
                mock_pysus,
                [f1],
                show_progress=False,
                as_dataframe=True,
                columns=["x", "y"],
            )
            assert list(result.columns) == ["x", "y"]

        asyncio.run(_run())


class TestFetchDucklake:

    def test_download_false_without_bag_returns_paths(self):
        async def _run():
            with patch("pysus.api.client.PySUS") as mock_cls:
                mock_pysus = MagicMock()
                mock_cls.return_value.__aenter__ = AsyncMock(
                    return_value=mock_pysus,
                )
                mock_cls.return_value.__aexit__ = AsyncMock()

                f1 = MagicMock()
                f1.path = "public/data/ftp/sinan/a.parquet"
                mock_pysus.query = AsyncMock(return_value=[f1])
                mock_pysus.cachepath = Path("/tmp/test_cache")

                from pysus.api._impl.databases import _fetch_ducklake

                result = await _fetch_ducklake(
                    mock_pysus,
                    dataset="sinan",
                    origin="FTP",
                    download=False,
                )
                assert result == ["public/data/ftp/sinan/a.parquet"]
                mock_pysus.query.assert_awaited_once()

        asyncio.run(_run())


class TestFlatDeprecationWarns:
    """Flat calls warn but still call _fetch_data with identical args."""

    def test_flat_sinan_warns_and_passes_through(self):
        import warnings

        from pysus.api._impl.databases import sinan

        with patch("pysus.api._impl.databases._fetch_data") as mock_fetch:
            mock_fetch.return_value = MagicMock()
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                sinan(disease="dengue", year=2024)
        assert any(x.category is PySUSWarning for x in w)
        args = mock_fetch.call_args
        assert args.kwargs["dataset"] == "sinan"
        assert args.kwargs["group"] == "DENGUE"
        assert args.kwargs["year"] == 2024

    def test_flat_list_files_warns_and_returns(self):
        import warnings

        from pysus.api._impl.databases import list_files

        async def _q(**kwargs):
            return []

        with patch.object(PySUS, "query", new=AsyncMock(side_effect=_q)):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                df = list_files("SINAN", year=2024)
            assert isinstance(df, pd.DataFrame)
        assert any(x.category is PySUSWarning for x in w)
