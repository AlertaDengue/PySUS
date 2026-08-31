"""High-level file-collection entity for the public API.

:class:`FileBag` wraps either a list of
:class:`~pysus.api.models.BaseRemoteFile` (nothing downloaded yet) or a list
of :class:`~pysus.api.models.BaseLocalFile` (already local, e.g. Parquet,
CSV, DBC) into a single, typed container.

The bag is a *high-level* surface: synchronous ``download()`` hides the async
machinery of the underlying ``PySUS`` client.  It is returned by the
origin-namespaced fetchers (``pysus.ftp.*``, ``pysus.dadosgov.*``,
``pysus.saude.*``); the flat, deprecated fetchers keep their historic
``list[str] | pd.DataFrame`` return type.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from pysus.api.client import _run_sync
from pysus.api.models import BaseLocalFile, BaseRemoteFile

if TYPE_CHECKING:
    import pandas as pd

F = TypeVar("F", bound="BaseRemoteFile | BaseLocalFile")

__all__ = ["FileBag"]


class FileBag(Generic[F]):
    """A high-level collection of one or more files.

    ``F`` is either :class:`~pysus.api.models.BaseRemoteFile` (files are not
    downloaded yet) or :class:`~pysus.api.models.BaseLocalFile` (files are
    already on disk).  All public methods are synchronous; any async download
    machinery is hidden inside the bag.
    """

    __slots__ = ("_files",)

    def __init__(self, files: list[F] | tuple[F, ...]) -> None:
        self._files: tuple[F, ...] = tuple(files)

    # -- introspection ---------------------------------------------------

    @property
    def files(self) -> tuple[F, ...]:
        """The underlying file entities, frozen as a tuple."""
        return self._files

    @property
    def kind(self) -> str:
        """``"remote"`` for remote files, ``"local"`` otherwise."""
        return _kind_of(self._files)

    @property
    def paths(self) -> list[str]:
        """Local/repo cache paths, or remote keys if still remote."""
        return [_path_str(f) for f in self._files]

    def __len__(self) -> int:
        return len(self._files)

    def __iter__(self) -> Iterator[F]:
        return iter(self._files)

    def __getitem__(self, index: int | slice) -> F | FileBag[F]:
        if isinstance(index, slice):
            return FileBag(list(self._files[index]))
        return self._files[index]

    def __repr__(self) -> str:
        remote = self.kind == "remote"
        entries = [
            f"{_name_str(f)} (remote)" if remote else _name_str(f)
            for f in self._files
        ]
        return f"Files[{', '.join(entries)}]"

    # -- download -------------------------------------------------------

    def download(
        self,
        indexes: list[int] | tuple[int, ...] | None = None,
    ) -> FileBag[BaseLocalFile]:
        """Download the (remote) files and return a local ``FileBag``.

        If the bag already holds local files this is a no-op and returns
        ``self``.  ``indexes`` optionally selects a subset of files (defaults
        to all).  Runs synchronously; the async ``PySUS`` download machinery
        is started and awaited internally.
        """
        if self.kind == "local":
            return cast("FileBag[BaseLocalFile]", self)

        selected = _select(self._files, indexes)
        local = cast(
            list[BaseLocalFile],
            _run_sync(_download_many(cast(list[BaseRemoteFile], selected))),
        )
        return FileBag(local)

    def download_one(self, index: int = 0) -> BaseLocalFile:
        """Download a single file and return the local file entity."""
        return self.download(indexes=[index]).files[0]

    # -- tabular convenience ---------------------------------------------

    def to_dataframe(self) -> pd.DataFrame:
        """Concatenate all local tabular files into one DataFrame.

        Only meaningful when the bag holds local files; remote files must be
        downloaded first.
        """
        if self.kind == "remote":
            raise ValueError(
                "Cannot build a DataFrame from a remote FileBag; call "
                "download() first."
            )
        import pandas as pd

        frames = cast(list[pd.DataFrame], _run_sync(_load_frames(self._files)))
        return (
            pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        )

    @property
    def df(self) -> pd.DataFrame:
        """Alias for :meth:`to_dataframe` (concatenated local frames)."""
        return self.to_dataframe()

    @property
    def first(self) -> F:
        """The first file entity."""
        return self._files[0]


# ── internal helpers ──────────────────────────────────────────────────


def _path_str(f: object) -> str:
    path = getattr(f, "path", None)
    if path is None:
        return _name_str(f)
    if isinstance(path, (str, bytes)):
        return str(path)
    fs = getattr(path, "__fspath__", None)
    if fs is not None:
        return fs()
    return str(path)


def _name_str(f: object) -> str:
    name = getattr(f, "name", None) or getattr(f, "basename", None)
    if name is not None:
        return str(name)
    path = getattr(f, "path", None)
    if path is not None:
        base = getattr(path, "name", None) or path
        return str(base).rsplit("/", 1)[-1]
    return repr(f)


def _kind_of(files: tuple[object, ...]) -> str:
    if not files:
        return "local"
    return "local" if isinstance(files[0], BaseLocalFile) else "remote"


def _select(
    files: tuple[object, ...],
    indexes: list[int] | tuple[int, ...] | None,
) -> list[object]:
    if indexes is None:
        return list(files)
    return [files[i] for i in indexes]


async def _download_many(files: list[BaseRemoteFile]) -> list[BaseLocalFile]:
    return [await f.download() for f in files]


async def _load_frames(files: tuple[object, ...]) -> list[pd.DataFrame]:
    import pandas as pd

    frames: list[pd.DataFrame] = []
    for f in files:
        if not isinstance(f, BaseLocalFile):
            continue
        data = await f.load()
        if isinstance(data, pd.DataFrame):
            frames.append(data)
    return frames


class _RemoteURL:
    """Minimal remote-file stand-in for URL-only origins (e.g. Saude).

    Holds a remote CSV/download URL in :attr:`path` and knows how to fetch it
    into a local file through :func:`download_http`.  It is intentionally
    not a ``BaseRemoteFile`` subclass (whose ``path`` is a local ``Path``);
    it exists so URL-backed downloads still surface as an item in a
    ``FileBag``.
    """

    __slots__ = ("path",)

    def __init__(self, url: str) -> None:
        self.path = url

    @property
    def basename(self) -> str:
        return self.path.rsplit("/", 1)[-1] or self.path

    name = basename

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"_RemoteURL({self.path!r})"

    async def download(self) -> BaseLocalFile:
        from pathlib import Path

        import httpx
        from pysus.api.extensions import ExtensionFactory

        dest = Path(Path(__import__("tempfile").gettempdir())) / self.basename
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(self.path)
            resp.raise_for_status()
            dest.write_bytes(resp.content)
        return await ExtensionFactory.instantiate(dest)
