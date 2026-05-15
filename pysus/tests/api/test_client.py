import pathlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pysus.api.client import DownloadStatus, LocalFileState, PySUS


@pytest.fixture
def test_db_path(tmp_path):
    return tmp_path / "test_config.db"


class TestPySUS:
    @pytest.mark.asyncio
    async def test_pysus_init(self, test_db_path):
        client = PySUS(db_path=test_db_path)
        assert client.cachepath == test_db_path.parent
        assert client._ducklake is None
        assert client._ftp is None
        assert client._dadosgov is None
        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_get_dest_path_basic(self, test_db_path):
        client = PySUS(db_path=test_db_path)

        mock_file = MagicMock()
        mock_file.client.name = "FTP"
        mock_file.dataset.name = "SINASC"
        mock_file.basename = "DNAC2024.dbc"
        mock_file.group = None

        result = client._get_dest_path(mock_file)
        expected = (
            test_db_path.parent
            / "downloads"
            / "ftp"
            / "sinasc"
            / "DNAC2024.dbc"
        )
        assert result == expected
        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_get_dest_path_with_group(self, test_db_path):
        client = PySUS(db_path=test_db_path)

        mock_file = MagicMock()
        mock_file.client.name = "FTP"
        mock_file.dataset.name = "SINASC"
        mock_file.basename = "DNAC2024.dbc"
        mock_group = MagicMock()
        mock_group.name = "DC"
        mock_file.group = mock_group

        result = client._get_dest_path(mock_file)
        expected = (
            test_db_path.parent
            / "downloads"
            / "ftp"
            / "sinasc"
            / "DC"
            / "DNAC2024.dbc"
        )
        assert result == expected
        await client.__aexit__(None, None, None)


class TestDownloadStatus:
    def test_download_status_values(self):
        assert DownloadStatus.PENDING.value == "pending"
        assert DownloadStatus.DOWNLOADING.value == "downloading"
        assert DownloadStatus.COMPLETED.value == "completed"
        assert DownloadStatus.FAILED.value == "failed"
        assert DownloadStatus.MISSING.value == "missing"


class TestLocalFileState:
    @pytest.mark.asyncio
    async def test_update_state_creates_record(self, test_db_path):
        client = PySUS(db_path=test_db_path)

        await client._update_state(
            local_path=pathlib.Path("/tmp/test.dbc"),
            remote_path="/remote/test.dbc",
            client_name="ftp",
            status=DownloadStatus.COMPLETED,
            year=2024,
            month=1,
            state="SP",
            group="DC",
        )

        with client.Session() as session:
            record = (
                session.query(LocalFileState)
                .filter_by(path="/tmp/test.dbc")
                .first()
            )
            assert record is not None
            assert record.remote_path == "/remote/test.dbc"
            assert record.client_name == "ftp"
            assert record.status == DownloadStatus.COMPLETED
            assert record.year == 2024
            assert record.month == 1
            assert record.state == "SP"
            assert record.group == "DC"

        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_delete_record_removes_entry(self, test_db_path):
        client = PySUS(db_path=test_db_path)

        await client._update_state(
            local_path=pathlib.Path("/tmp/test.dbc"),
            remote_path="/remote/test.dbc",
            client_name="ftp",
            status=DownloadStatus.COMPLETED,
        )

        await client._delete_record("/tmp/test.dbc")

        with client.Session() as session:
            record = (
                session.query(LocalFileState)
                .filter_by(path="/tmp/test.dbc")
                .first()
            )
            assert record is None

        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_get_local_file_finds_existing(self, test_db_path):
        client = PySUS(db_path=test_db_path)

        await client._update_state(
            local_path=pathlib.Path("/tmp/test.dbc"),
            remote_path="/remote/test.dbc",
            client_name="ftp",
            status=DownloadStatus.COMPLETED,
        )

        mock_remote_file = MagicMock()
        mock_remote_file.client.name = "FTP"
        mock_remote_file.path = "/remote/test.dbc"

        with patch(
            "pysus.api.extensions.ExtensionFactory.instantiate"
        ) as mock_factory:
            mock_factory.return_value = MagicMock()
            await client.get_local_file(mock_remote_file)
            mock_factory.assert_called_once()

        await client.__aexit__(None, None, None)


class TestGetCompletedRemotePaths:
    @pytest.mark.asyncio
    async def test_get_completed_remote_paths(self, test_db_path):
        client = PySUS(db_path=test_db_path)

        await client._update_state(
            local_path=pathlib.Path("/tmp/test1.dbc"),
            remote_path="/remote/test1.dbc",
            client_name="ftp",
            status=DownloadStatus.COMPLETED,
        )
        await client._update_state(
            local_path=pathlib.Path("/tmp/test2.dbc"),
            remote_path="/remote/test2.dbc",
            client_name="ftp",
            status=DownloadStatus.PENDING,
        )

        paths = client.get_completed_remote_paths()
        assert "/remote/test1.dbc" in paths
        assert "/remote/test2.dbc" not in paths

        await client.__aexit__(None, None, None)


class TestPySUSQuery:
    @pytest.mark.asyncio
    async def test_query_with_dataset(self, test_db_path, tmp_path):
        from unittest.mock import AsyncMock, MagicMock

        from pysus.api.ducklake.client import DuckLake

        client = PySUS(db_path=test_db_path)

        mock_ducklake = MagicMock(spec=DuckLake)
        mock_file = MagicMock()
        mock_file.path = tmp_path / "test.parquet"
        mock_ducklake.query = AsyncMock(return_value=[mock_file])

        client._ducklake = mock_ducklake
        client._attach_client_catalog = MagicMock()

        result = await client.query(dataset="sinan")

        mock_ducklake.query.assert_called_once_with(
            dataset="sinan",
            group=None,
            state=None,
            year=None,
            month=None,
        )
        assert result == [mock_file]
        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_query_with_group(self, test_db_path):
        from unittest.mock import AsyncMock, MagicMock

        from pysus.api.ducklake.client import DuckLake

        client = PySUS(db_path=test_db_path)

        mock_ducklake = MagicMock(spec=DuckLake)
        mock_ducklake.query = AsyncMock(return_value=[])

        client._ducklake = mock_ducklake
        client._attach_client_catalog = MagicMock()

        await client.query(dataset="sinan", group="DENGUE")

        mock_ducklake.query.assert_called_once_with(
            dataset="sinan",
            group="DENGUE",
            state=None,
            year=None,
            month=None,
        )
        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_query_with_all_params(self, test_db_path):
        from unittest.mock import AsyncMock, MagicMock

        from pysus.api.ducklake.client import DuckLake

        client = PySUS(db_path=test_db_path)

        mock_ducklake = MagicMock(spec=DuckLake)
        mock_ducklake.query = AsyncMock(return_value=[])

        client._ducklake = mock_ducklake
        client._attach_client_catalog = MagicMock()

        await client.query(
            dataset="sinasc",
            group="DC",
            state="SP",
            year=2024,
            month=1,
        )

        mock_ducklake.query.assert_called_once_with(
            dataset="sinasc",
            group="DC",
            state="SP",
            year=2024,
            month=1,
        )
        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_query_initializes_ducklake(self, test_db_path):
        from unittest.mock import AsyncMock, MagicMock, patch

        import duckdb
        from pysus.api.ducklake.client import DuckLake

        client = PySUS(db_path=test_db_path)
        assert client._ducklake is None

        mock_ducklake_instance = MagicMock(spec=DuckLake)
        mock_ducklake_instance.query = AsyncMock(return_value=[])
        tmp_catalog_path = test_db_path.parent / "catalog.db"
        mock_ducklake_instance.catalog_path = tmp_catalog_path

        # Create the catalog database
        conn = duckdb.connect(str(tmp_catalog_path))
        conn.close()

        with patch.object(
            DuckLake, "__new__", return_value=mock_ducklake_instance
        ):
            await client.query(dataset="sinan")

        assert client._ducklake is not None
        await client.__aexit__(None, None, None)


class TestDownload:
    @pytest.mark.asyncio
    async def test_download_returns_existing_when_size_matches(
        self, test_db_path
    ):
        from unittest.mock import AsyncMock, MagicMock, patch

        client = PySUS(db_path=test_db_path)
        mock_local = MagicMock()
        mock_local.path.exists.return_value = True
        mock_local.size = 1000
        mock_file = MagicMock()
        mock_file.size = 1000
        mock_file.client.name = "ftp"

        with patch.object(
            client, "get_local_file", new=AsyncMock(return_value=mock_local)
        ):
            result = await client.download(mock_file)

        assert result == mock_local

    @pytest.mark.asyncio
    async def test_download_re_fetches_when_size_differs(self, test_db_path):
        import pathlib
        from unittest.mock import AsyncMock, MagicMock, patch

        from pysus.api.extensions import ExtensionFactory

        mock_local = MagicMock()
        mock_local.path.exists.return_value = True
        mock_local.size = 500
        mock_file = MagicMock()
        mock_file.size = 1000
        mock_file.client.name = "ftp"
        mock_file.path = pathlib.Path("/remote/test.dbc")
        mock_file.basename = "test.dbc"

        client = PySUS(db_path=test_db_path)
        get_local_file_patch = patch.object(
            client, "get_local_file", new=AsyncMock(return_value=mock_local)
        )
        delete_record_patch = patch.object(
            client, "_delete_record", new=AsyncMock()
        )
        get_dest_patch = patch.object(
            client,
            "_get_dest_path",
            return_value=test_db_path.parent / "test.dbc",
        )
        update_state_patch = patch.object(
            client, "_update_state", new=AsyncMock()
        )
        get_ftp_patch = patch.object(client, "get_ftp", new=AsyncMock())

        with (
            get_local_file_patch,
            delete_record_patch as mock_delete,
            get_dest_patch,
            update_state_patch,
            get_ftp_patch,
        ):
            with patch.object(
                ExtensionFactory, "instantiate", return_value=mock_local
            ):
                mock_client = AsyncMock()
                mock_client._download_file = AsyncMock()
                client._ftp = mock_client
                await client.download(mock_file)

        mock_delete.assert_awaited_once()
        assert mock_local.path.unlink.called

    @pytest.mark.asyncio
    async def test_download_passes_timeout(self, test_db_path):
        from unittest.mock import AsyncMock, MagicMock, patch

        import anyio
        from pysus.api.extensions import ExtensionFactory

        mock_local = MagicMock()
        mock_local.path.exists.return_value = False
        mock_file = MagicMock()
        mock_file.size = 1000
        mock_file.client.name = "ftp"
        mock_file.path = test_db_path.parent / "remote.dbc"
        mock_file.basename = "remote.dbc"

        client = PySUS(db_path=test_db_path)

        async def _slow_download(*args, **kwargs):
            await anyio.sleep(10)

        with (
            patch.object(
                client, "get_local_file", new=AsyncMock(return_value=mock_local)
            ),
            patch.object(
                client,
                "_get_dest_path",
                return_value=test_db_path.parent / "test.dbc",
            ),
            patch.object(client, "_update_state", new=AsyncMock()),
            patch.object(
                ExtensionFactory, "instantiate", return_value=mock_local
            ),
        ):
            mock_client = AsyncMock()
            mock_client._download_file = _slow_download
            client._ftp = mock_client

            with pytest.raises(
                RuntimeError, match="Unexpected error downloading"
            ):
                await client.download(mock_file, timeout=0.001)


class TestReadParquet:
    def test_read_parquet_single_path(self, tmp_path):
        import pandas as pd

        parquet_file = tmp_path / "test.parquet"
        pd.DataFrame({"a": [1, 2], "b": [3, 4]}).to_parquet(parquet_file)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet([parquet_file])
        df = result.df()

        assert len(df) == 2
        assert "a" in df.columns
        assert "b" in df.columns

    def test_read_parquet_union_mode(self, tmp_path):
        import pandas as pd

        parquet1 = tmp_path / "test1.parquet"
        parquet2 = tmp_path / "test2.parquet"

        pd.DataFrame({"a": [1], "b": [2]}).to_parquet(parquet1)
        pd.DataFrame({"a": [3], "c": [4]}).to_parquet(parquet2)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet([parquet1, parquet2], mode="union")
        df = result.df()

        assert len(df) == 2
        assert "a" in df.columns
        assert "b" in df.columns
        assert "c" in df.columns

    def test_read_parquet_intersection_mode(self, tmp_path):
        import pandas as pd

        parquet1 = tmp_path / "test1.parquet"
        parquet2 = tmp_path / "test2.parquet"

        pd.DataFrame({"a": [1], "b": [2]}).to_parquet(parquet1)
        pd.DataFrame({"a": [3], "c": [4]}).to_parquet(parquet2)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet([parquet1, parquet2], mode="intersection")
        df = result.df()

        assert len(df) == 2
        assert list(df.columns) == ["a"]

    def test_read_parquet_strict_mode_matching_schemas(self, tmp_path):
        import pandas as pd

        parquet1 = tmp_path / "test1.parquet"
        parquet2 = tmp_path / "test2.parquet"

        pd.DataFrame({"a": [1], "b": [2]}).to_parquet(parquet1)
        pd.DataFrame({"a": [3], "b": [4]}).to_parquet(parquet2)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet([parquet1, parquet2], mode="strict")
        df = result.df()

        assert len(df) == 2

    def test_read_parquet_strict_mode_mismatching_schemas(self, tmp_path):
        import pandas as pd

        parquet1 = tmp_path / "test1.parquet"
        parquet2 = tmp_path / "test2.parquet"

        pd.DataFrame({"a": [1], "b": [2]}).to_parquet(parquet1)
        pd.DataFrame({"a": [3], "c": [4]}).to_parquet(parquet2)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")

        with pytest.raises(ValueError, match="Schema mismatch"):
            client.read_parquet([parquet1, parquet2], mode="strict")

    def test_read_parquet_with_sql(self, tmp_path):
        import pandas as pd

        parquet_file = tmp_path / "test.parquet"
        pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).to_parquet(parquet_file)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet(
            [parquet_file], sql="SELECT a FROM t WHERE a > 1"
        )
        df = result.df()

        assert len(df) == 2
        assert list(df.columns) == ["a"]

    def test_read_parquet_no_paths_raises(self, tmp_path):
        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")

        with pytest.raises(ValueError, match="No paths provided"):
            client.read_parquet([])

    def test_read_parquet_add_dv_applies_verification_digit(self, tmp_path):
        import pandas as pd

        parquet_file = tmp_path / "test.parquet"
        df = pd.DataFrame({"ID_MUNICIP": ["261160", "530010"], "value": [1, 2]})
        df.to_parquet(parquet_file)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet([parquet_file], add_dv=True)
        out = result.df()

        assert out["ID_MUNICIP"].iloc[0] == "2611606"
        assert out["ID_MUNICIP"].iloc[1] == "5300108"

    def test_read_parquet_add_dv_skips_no_geocode_columns(self, tmp_path):
        import pandas as pd

        parquet_file = tmp_path / "test.parquet"
        df = pd.DataFrame({"DT_NOTIFIC": ["20230101"], "value": [1]})
        df.to_parquet(parquet_file)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet([parquet_file], add_dv=True)
        out = result.df()

        assert list(out.columns) == ["DT_NOTIFIC", "value"]

    def test_read_parquet_add_dv_false_returns_raw(self, tmp_path):
        import pandas as pd

        parquet_file = tmp_path / "test.parquet"
        df = pd.DataFrame({"ID_MUNICIP": ["261160"], "value": [1]})
        df.to_parquet(parquet_file)

        from pysus.api.client import PySUS

        client = PySUS(db_path=tmp_path / "config.db")
        result = client.read_parquet([parquet_file], add_dv=False)

        from duckdb import DuckDBPyConnection

        assert isinstance(result, DuckDBPyConnection)
        out = result.df()
        assert out["ID_MUNICIP"].iloc[0] == "261160"


class TestPySUSGetMethods:
    @pytest.mark.asyncio
    async def test_get_dadosgov(self, test_db_path):
        from pysus.api.dadosgov import DadosGovClient

        client = PySUS(db_path=test_db_path)
        assert client._dadosgov is None

        with patch.object(
            DadosGovClient, "connect", new_callable=AsyncMock
        ) as mock_connect:
            result = await client.get_dadosgov("test_token")
            assert result is not None
            assert client._dadosgov is not None
            mock_connect.assert_called_once()

        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_get_ftp(self, test_db_path):
        from pysus.api.ftp import FTPClient

        client = PySUS(db_path=test_db_path)
        assert client._ftp is None

        with patch.object(
            FTPClient, "connect", new_callable=AsyncMock
        ) as mock_connect:
            result = await client.get_ftp()
            assert result is not None
            assert client._ftp is not None
            mock_connect.assert_called_once()

        await client.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_aenter(self, test_db_path):
        from pysus.api.ducklake.client import DuckLake

        client = PySUS(db_path=test_db_path)

        with (
            patch.object(
                DuckLake, "_load_catalog", new_callable=AsyncMock
            ) as mock_load,
            patch.object(PySUS, "_attach_client_catalog") as mock_attach,
        ):
            await client.__aenter__()
            assert client._ducklake is not None
            mock_load.assert_called_once()
            mock_attach.assert_called_once()

        await client.__aexit__(None, None, None)
