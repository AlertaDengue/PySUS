from unittest.mock import MagicMock, patch, AsyncMock
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
            assert args.kwargs["group_filter"] == "DENGUE"
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
            mock_pysus_class.return_value.__aenter__ = AsyncMock(return_value=mock_pysus)
            mock_pysus_class.return_value.__aexit__ = AsyncMock()

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
            mock_pysus_class.return_value.__aenter__ = AsyncMock(return_value=mock_pysus)
            mock_pysus_class.return_value.__aexit__ = AsyncMock()

            mock_file = MagicMock()
            mock_file.path = "/tmp/test.parquet"
            mock_pysus.query = AsyncMock(return_value=[mock_file])
            mock_pysus.download = AsyncMock(return_value=mock_file)
            mock_pysus.read_parquet.return_value.df.return_value = MagicMock()

            from pysus.api._impl.databases import _fetch_data
            _fetch_data(dataset="sinan", year=[2023, 2024], show_progress=False)

            assert mock_pysus.query.call_count == 2

    def test_fetch_data_with_group_filter(self):
        with patch("pysus.api._impl.databases.PySUS") as mock_pysus_class:
            mock_pysus = MagicMock()
            mock_pysus_class.return_value.__aenter__ = AsyncMock(return_value=mock_pysus)
            mock_pysus_class.return_value.__aexit__ = AsyncMock()

            mock_file = MagicMock()
            mock_file.path = "/tmp/test.parquet"
            mock_pysus.query = AsyncMock(return_value=[mock_file])
            mock_pysus.download = AsyncMock(return_value=mock_file)
            mock_pysus.read_parquet.return_value.df.return_value = MagicMock()

            from pysus.api._impl.databases import _fetch_data
            _fetch_data(dataset="sinan", group_filter="DENGUE", state="SP", show_progress=False)

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
            mock_pysus_class.return_value.__aenter__ = AsyncMock(return_value=mock_pysus)
            mock_pysus_class.return_value.__aexit__ = AsyncMock()

            mock_pysus.query = AsyncMock(return_value=[])

            import pandas as pd
            from pysus.api._impl.databases import _fetch_data
            result = _fetch_data(dataset="sinan", year=2024, show_progress=False)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0