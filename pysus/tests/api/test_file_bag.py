"""Phase FileBag tests - high-level FileBag entity.

Covers:
- FileBag wraps remote and local file entities;
- sync ``download()`` / ``download_one()`` / ``download(indexes=)``;
- ``to_dataframe()`` / ``df`` over local tabular files;
- subsetting via ``__getitem__`` / ``__len__`` / ``__iter__`` / ``paths``;
- namespaced fetchers return a FileBag (download=False -> remote bag);
- ``as_dataframe=True`` still yields a plain DataFrame.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from pysus.api.bag import FileBag
from pysus.api.client import PySUS, _run_sync
from pysus.api.extensions import ExtensionFactory
from pysus.api.models import BaseLocalFile


def _local(path: Path) -> BaseLocalFile:
    return _run_sync(ExtensionFactory.instantiate(path))


def _remotable(path: str, target: Path | None = None):
    """Lightweight remote-file stand-in with ``path`` and async ``download``."""

    class _RemoteStub:
        path: Path

        def __init__(self, key):
            self.path = Path(key)

        async def download(self):
            if target is None:
                raise AssertionError("unexpected download")
            return await ExtensionFactory.instantiate(target)

    return _RemoteStub(path)


@pytest.fixture()
def pysus():
    import pysus  # noqa: F401

    return pysus


def _make_parquet(path: Path, rows: int = 3) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"a": list(range(rows))}).to_parquet(path)
    return path


class TestFileBagBuilding:
    def test_remote_kind_and_paths(self):
        bag = FileBag([_remotable("public/data/ftp/sinan/a.parquet")])
        assert bag.kind == "remote"
        assert not isinstance(bag.files[0], BaseLocalFile)
        assert bag.paths == ["public/data/ftp/sinan/a.parquet"]

    def test_repr_remote_marks_files_as_remote(self):
        bag = FileBag(
            [
                _remotable(
                    "public/data/ftp/sinan/DENG/2020/BR/DENGBR20.parquet"
                ),
                _remotable(
                    "public/data/ftp/sinan/DENG/2021/BR/DENGBR21.parquet"
                ),
            ]
        )
        assert (
            repr(bag)
            == "Files[DENGBR20.parquet (remote), DENGBR21.parquet (remote)]"
        )

    def test_repr_local_omits_flag(self, tmp_path):
        bag = FileBag([_local(_make_parquet(tmp_path / "a.parquet", rows=1))])
        assert repr(bag) == "Files[a.parquet]"

    def test_empty_is_treated_as_local(self):
        bag = FileBag([])
        assert len(bag) == 0
        assert bag.kind == "local"

    def test_local_bag_kind(self, tmp_path):
        bag = FileBag([_local(_make_parquet(tmp_path / "a.parquet"))])
        assert bag.kind == "local"
        assert isinstance(bag.files[0], BaseLocalFile)

    def test_len_iter_getitem(self, tmp_path):
        p1 = _make_parquet(tmp_path / "a.parquet")
        p2 = _make_parquet(tmp_path / "b.parquet")
        bag = FileBag([_local(p1), _local(p2)])

        assert len(bag) == 2
        assert [f.path for f in bag] == [p1, p2]
        assert bag[0].path == p1
        assert isinstance(bag[0:1], FileBag)
        assert bag.first.path == p1


class TestDownload:
    def test_download_converts_remote_to_local(self, tmp_path):
        local = _make_parquet(tmp_path / "DENGBR20.parquet")
        bag = FileBag(
            [
                _remotable(
                    "public/data/ftp/sinan/DENG/2020/_/BR/DENGBR20.parquet",
                    local,
                )
            ]
        )
        result = bag.download()
        assert result.kind == "local"
        assert result.paths == [str(local).replace("\\", "/")]
        assert len(result) == 1

    def test_download_one(self, tmp_path):
        local = _make_parquet(tmp_path / "x.parquet")
        bag = FileBag([_remotable("public/data/x.parquet", local)])
        lf = bag.download_one(0)
        assert isinstance(lf, BaseLocalFile)

    def test_download_subset(self, tmp_path):
        locals_ = [
            _make_parquet(tmp_path / "a.parquet", rows=1),
            _make_parquet(tmp_path / "b.parquet", rows=1),
        ]
        bag = FileBag(
            [
                _remotable(f"public/data/{i}.parquet", target)
                for i, target in enumerate(locals_)
            ]
        )
        subset = bag.download(indexes=[1])
        assert subset.paths == [str(locals_[1]).replace("\\", "/")]

    def test_local_bag_download_is_noop(self, tmp_path):
        bag = FileBag([_local(_make_parquet(tmp_path / "a.parquet"))])
        assert bag.download() is bag


class TestDataFrame:
    def test_to_dataframe_concatenates(self, tmp_path):
        p1 = _make_parquet(tmp_path / "a.parquet", rows=2)
        p2 = _make_parquet(tmp_path / "b.parquet", rows=3)
        bag = FileBag([_local(p1), _local(p2)])
        df = bag.to_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert bag.df.equals(df)

    def test_remote_bag_to_dataframe_raises(self):
        bag = FileBag([_remotable("public/data/x.parquet")])
        with pytest.raises(ValueError):
            bag.to_dataframe()


class TestNamespacedReturn:
    def test_download_false_returns_remote_file_bag(self, pysus):
        import pysus.tests.api.test_origins as origins

        files = [
            origins._StubFile(
                "public/data/ftp/sinan/DENG/2017/_/BR/DENGBR17.parquet"
            )
        ]
        with patch.object(PySUS, "query", new_callable=AsyncMock) as query:
            query.return_value = files
            result = pysus.ftp.sinan(disease="deng", year=2017, download=False)
        assert isinstance(result, FileBag)
        assert result.kind == "remote"
        assert result.paths == [
            "public/data/ftp/sinan/DENG/2017/_/BR/DENGBR17.parquet"
        ]

    def test_as_dataframe_still_returns_dataframe(self, pysus):
        import pysus.tests.api.test_origins as origins

        class _Reader:
            def df(self):
                return pd.DataFrame({"a": [1, 2]})

        files = [origins._StubFile("public/data/ftp/sinan/a.parquet")]
        with (
            patch.object(PySUS, "query", new_callable=AsyncMock) as query,
            patch.object(
                PySUS,
                "download",
                new_callable=AsyncMock,
                return_value=origins._StubFile("x"),
            ),
            patch.object(PySUS, "read_parquet", return_value=_Reader()),
        ):
            query.return_value = files
            result = pysus.ftp.sinan(
                disease="deng", year=2017, as_dataframe=True
            )
        assert isinstance(result, pd.DataFrame)
        assert list(result["a"]) == [1, 2]


class TestBagHelpers:
    def test_path_str_falls_back_to_name(self):
        from pysus.api.bag import _path_str

        class _F:
            path = None
            name = "only-name.csv"

        assert _path_str(_F()) == "only-name.csv"

    def test_name_str_falls_back_to_repr(self):
        from pysus.api.bag import _name_str

        f = object()
        assert _name_str(f) == repr(f)

    def test_load_frames_skips_non_local_files(self):
        from pysus.api.bag import _load_frames

        frames = _run_sync(_load_frames((MagicMock(),)))
        assert frames == []

    def test_path_str_normalizes_windows_separators(self):
        from pysus.api.bag import _path_str

        class _F:
            path = "public\\data\\ftp\\sinan\\a.parquet"

        assert _path_str(_F()) == "public/data/ftp/sinan/a.parquet"

    def test_path_str_preserves_posix_separators(self):
        from pysus.api.bag import _path_str

        class _F:
            path = "public/data/ftp/sinan/a.parquet"

        assert _path_str(_F()) == "public/data/ftp/sinan/a.parquet"

    def test_remote_url_basename(self):
        from pysus.api.bag import _RemoteURL

        assert _RemoteURL("http://example.com/a.csv").basename == "a.csv"
        assert (
            _RemoteURL("http://example.com/").basename == "http://example.com/"
        )
        assert _RemoteURL("a.csv").basename == "a.csv"

    def test_remote_url_download(self):
        from pysus.api.bag import _RemoteURL

        class _FakeResp:
            content = b"parquet-bytes"

            def raise_for_status(self):
                pass

        class _FakeClient:
            def __init__(self, *args, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def get(self, url):
                return _FakeResp()

        fake_local = MagicMock()
        with (
            patch("httpx.AsyncClient", return_value=_FakeClient()),
            patch(
                "pysus.api.extensions.ExtensionFactory.instantiate",
                return_value=fake_local,
            ) as inst,
        ):
            result = _run_sync(
                _RemoteURL("http://example.com/a.csv").download()
            )
        assert result is fake_local
        assert inst.called
