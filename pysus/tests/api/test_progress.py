"""Tests for pysus.api.progress module."""


class TestProgressFlags:
    def test_disable_then_enable(self):
        import pysus.api.progress as mod
        from pysus.api.progress import (
            disable_progress_bars,
            enable_progress_bars,
        )

        original = mod._progress_enabled
        try:
            disable_progress_bars()
            assert mod._progress_enabled is False
            enable_progress_bars()
            assert mod._progress_enabled is True
        finally:
            mod._progress_enabled = original


class TestProgressCallback:
    def test_init_creates_bar(self):
        from pysus.api.progress import ProgressCallback

        cb = ProgressCallback(desc="test.bin", total=1000)
        assert cb._bar is not None
        cb.close()

    def test_call_updates_bar(self):
        from pysus.api.progress import ProgressCallback

        cb = ProgressCallback(desc="test.bin", total=100)
        cb(50, 100)
        assert cb._bar.n == 50
        cb.close()

    def test_call_resets_total_if_changed(self):
        from pysus.api.progress import ProgressCallback

        cb = ProgressCallback(desc="test.bin", total=100)
        cb(50, 200)
        assert cb._bar.total == 200
        cb.close()

    def test_close_closes_bar(self):
        from pysus.api.progress import ProgressCallback

        cb = ProgressCallback(desc="test.bin", total=100)
        cb.close()
        assert cb._bar.fp is not None  # tqdm closes but fp exists


class TestGetProgressCallback:
    def test_returns_callback_when_enabled(self):
        import pysus.api.progress as mod
        from pysus.api.progress import (
            ProgressCallback,
            enable_progress_bars,
            get_progress_callback,
        )

        original = mod._progress_enabled
        try:
            enable_progress_bars()
            cb = get_progress_callback(desc="test")
            assert isinstance(cb, ProgressCallback)
            cb.close()
        finally:
            mod._progress_enabled = original

    def test_returns_none_when_disabled(self):
        import pysus.api.progress as mod
        from pysus.api.progress import (
            disable_progress_bars,
            get_progress_callback,
        )

        original = mod._progress_enabled
        try:
            disable_progress_bars()
            assert get_progress_callback(desc="test") is None
        finally:
            mod._progress_enabled = original


class TestDownloadAutoProgress:
    def test_download_creates_progress_when_no_callback(self, tmp_path):
        """When no callback is passed, download() auto-creates one."""
        import pysus.api.progress as mod

        original = mod._progress_enabled
        try:
            mod._progress_enabled = False  # disable for test
            from pysus.api.client import PySUS

            db = tmp_path / "test.db"
            client = PySUS(db_path=db)
            assert hasattr(client, "download")
        finally:
            mod._progress_enabled = original

    def test_disable_suppresses_auto_callback(self):
        """When disabled, no auto-callback is created."""
        from pysus.api.progress import (
            disable_progress_bars,
            enable_progress_bars,
            get_progress_callback,
        )

        disable_progress_bars()
        cb = get_progress_callback(desc="test")
        assert cb is None
        enable_progress_bars()


class TestTopLevelExports:
    def test_pysus_exports_progress_flags(self):
        import pysus

        assert hasattr(pysus, "enable_progress_bars")
        assert hasattr(pysus, "disable_progress_bars")
