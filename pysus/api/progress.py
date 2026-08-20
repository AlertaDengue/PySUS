"""Progress bar support for PySUS downloads.

Uses ``tqdm`` when available and enabled, otherwise falls back to a
silent no-op callback.

Examples
--------
>>> from pysus.api.progress import (
...     enable_progress_bars,
...     disable_progress_bars,
...     ProgressCallback,
... )
>>> disable_progress_bars()  # suppress bars globally
>>> enable_progress_bars()   # re-enable
"""

from __future__ import annotations

from collections.abc import Callable

_progress_enabled: bool = True


def enable_progress_bars() -> None:
    """Enable tqdm progress bars for downloads globally."""
    global _progress_enabled  # noqa: PLW0603
    _progress_enabled = True


def disable_progress_bars() -> None:
    """Disable tqdm progress bars for downloads globally."""
    global _progress_enabled  # noqa: PLW0603
    _progress_enabled = False


class _NullCallback:
    """Silent callback used when progress bars are disabled."""

    def __call__(self, downloaded: int, total: int) -> None:  # noqa: ARG002
        pass

    def set_total(self, total: int) -> None:  # noqa: ARG002
        pass

    def close(self) -> None:
        pass


class ProgressCallback:
    """A ``tqdm``-based progress callback compatible with ``download()``.

    Parameters
    ----------
    desc : str, optional
        Description prefix for the progress bar (e.g. filename).
    total : int, optional
        Total expected size in bytes.  Can be set later via
        ``set_total()``.
    """

    def __init__(
        self,
        desc: str | None = None,
        total: int | None = None,
    ) -> None:
        try:
            from tqdm import tqdm  # type: ignore[import-untyped]

            self._bar: tqdm | None = tqdm(
                total=total or 0,
                unit="B",
                unit_scale=True,
                desc=desc,
            )
        except ImportError:  # pragma: no cover
            self._bar = None

    def __call__(self, downloaded: int, total: int) -> None:
        if self._bar is None:
            return
        if self._bar.total != total:
            self._bar.reset(total=total)
        self._bar.n = downloaded
        self._bar.refresh()

    def set_total(self, total: int) -> None:
        if self._bar is not None:
            self._bar.reset(total=total)

    def close(self) -> None:
        if self._bar is not None:
            self._bar.close()


def get_progress_callback(
    desc: str | None = None,
) -> Callable[[int, int], None] | None:
    """Return a progress callback if bars are enabled, else ``None``.

    Parameters
    ----------
    desc : str, optional
        Description for the tqdm bar.

    Returns
    -------
    Callable or None
        A ``(downloaded, total) -> None`` callback, or ``None``.
    """
    if not _progress_enabled:
        return None
    return ProgressCallback(desc=desc)
