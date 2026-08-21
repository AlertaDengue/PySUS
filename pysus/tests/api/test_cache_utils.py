"""Tests for pysus.api.cache_utils — smart caching."""

from datetime import datetime, timedelta
from pathlib import Path

from pysus.api.cache_utils import (
    CacheStatus,
    cache_status,
    clear_cache,
    format_size,
    is_cache_fresh,
)


class TestCacheStatus:
    def test_empty_dir(self, tmp_path):
        status = cache_status(tmp_path)
        assert status.total_files == 0
        assert status.total_size_bytes == 0
        assert status.last_modified is None

    def test_with_files(self, tmp_path):
        (tmp_path / "a.parquet").write_bytes(b"x" * 100)
        (tmp_path / "b.csv").write_bytes(b"y" * 200)
        (tmp_path / "c.partial.parquet").write_bytes(b"z" * 50)
        status = cache_status(tmp_path)
        assert status.total_files == 3
        assert status.parquet_files == 2  # a.parquet + c.partial.parquet
        assert status.partial_files == 1
        assert status.total_size_bytes == 350
        assert status.last_modified is not None

    def test_size_mb(self):
        status = CacheStatus(
            path=Path("/tmp"),
            total_files=0,
            total_size_bytes=1024 * 1024 * 5,
            parquet_files=0,
            partial_files=0,
            last_modified=None,
        )
        assert status.total_size_mb == 5.0

    def test_nonexistent_dir(self, tmp_path):
        status = cache_status(tmp_path / "nonexistent")
        assert status.total_files == 0


class TestIsCacheFresh:
    def test_nonexistent_file(self, tmp_path):
        assert not is_cache_fresh(tmp_path / "missing.parquet")

    def test_no_remote_mtime(self, tmp_path):
        f = tmp_path / "data.parquet"
        f.write_bytes(b"data")
        assert is_cache_fresh(f)

    def test_local_newer(self, tmp_path):
        f = tmp_path / "data.parquet"
        f.write_bytes(b"data")
        remote_mtime = datetime.now() - timedelta(hours=1)
        assert is_cache_fresh(f, remote_mtime)

    def test_local_older(self, tmp_path):
        f = tmp_path / "data.parquet"
        f.write_bytes(b"data")
        # Set local mtime to old
        import os

        old_time = (datetime.now() - timedelta(days=2)).timestamp()
        os.utime(f, (old_time, old_time))
        remote_mtime = datetime.now()
        assert not is_cache_fresh(f, remote_mtime)


class TestClearCache:
    def test_clears_files(self, tmp_path):
        (tmp_path / "a.parquet").write_bytes(b"x")
        (tmp_path / "b.csv").write_bytes(b"y")
        count = clear_cache(tmp_path)
        assert count == 2
        assert not any(tmp_path.rglob("*"))

    def test_empty_dir(self, tmp_path):
        count = clear_cache(tmp_path)
        assert count == 0


class TestFormatSize:
    def test_bytes(self):
        assert format_size(100) == "100.0 B"

    def test_kb(self):
        assert format_size(1536) == "1.5 KB"

    def test_mb(self):
        assert format_size(1024 * 1024) == "1.0 MB"

    def test_gb(self):
        assert format_size(1024**3) == "1.0 GB"
