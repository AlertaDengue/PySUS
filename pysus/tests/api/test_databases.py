import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


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
        with patch("pysus.api._impl.databases.PySUS") as mock_pysus_class:
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
                dataset="sinan",
                group=None,
                state=None,
                year=2024,
                month=None,
            )

    def test_fetch_data_multiple_years(self):
        with patch("pysus.api._impl.databases.PySUS") as mock_pysus_class:
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
        with patch("pysus.api._impl.databases.PySUS") as mock_pysus_class:
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
                dataset="sinan",
                group="DENGUE",
                state="SP",
                year=None,
                month=None,
            )

    def test_fetch_data_empty_result(self):
        with patch("pysus.api._impl.databases.PySUS") as mock_pysus_class:
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
        with patch("pysus.api._impl.databases.PySUS") as mock_pysus_class:
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
        with patch("pysus.api._impl.databases.PySUS") as mock_pysus_class:
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
            patch("pysus.api._impl.databases.PySUS") as mock_pysus_class,
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
                patch("pysus.api._impl.databases.PySUS") as mock_pysus_class,
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

        with patch("pysus.api._impl.databases.PySUS") as mock_pysus_class:
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

        with patch("pysus.api._impl.databases.PySUS") as mock_pysus_class:
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

        with patch("pysus.api._impl.databases.PySUS") as mock_pysus_class:
            mock_pysus = MagicMock()
            mock_pysus_class.return_value.__aenter__ = AsyncMock(
                return_value=mock_pysus
            )
            mock_pysus_class.return_value.__aexit__ = AsyncMock()
            mock_pysus.query = AsyncMock(side_effect=[records[:2], records[2:]])

            from pysus.api._impl.databases import list_files

            result = list_files(dataset="SINAN", year=[2023, 2024])

            assert len(result) == 3
            assert mock_pysus.query.call_count == 2
