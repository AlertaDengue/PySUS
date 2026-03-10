"""
Lazy database wrapper to defer FTP connections until first use.

This avoids connecting to the FTP server on import, which can cause
hangs and failures when the server is unavailable.
"""

from loguru import logger


class _LazyDatabase:
    """Base lazy wrapper that defers FTP connection until the database is
    actually accessed. All attribute access is transparently proxied to
    the underlying database instance.

    Subclasses only need to override ``_ensure_loaded`` if custom
    initialisation logic is required; the default implementation calls
    ``db_class().load()`` with error handling for FTP failures.
    """

    def __init__(self, db_class):
        # Use object.__setattr__ to avoid triggering __getattr__
        object.__setattr__(self, "_db_class", db_class)
        object.__setattr__(self, "_instance", None)

    def _ensure_loaded(self):
        if self._instance is None:
            try:
                instance = self._db_class().load()
            except Exception as exc:
                logger.error(
                    "Failed to connect to FTP server for "
                    f"{self._db_class.__name__}: {exc}"
                )
                raise ConnectionError(
                    f"Could not load {self._db_class.__name__} database. "
                    "The FTP server may be unavailable. "
                    f"Original error: {exc}"
                ) from exc
            object.__setattr__(self, "_instance", instance)
        return self._instance

    def __getattr__(self, name):
        return getattr(self._ensure_loaded(), name)
