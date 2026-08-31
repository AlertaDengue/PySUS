"""pytest configuration - mocks duckdb.functional before any other imports.

Also silences the flat-API deprecation warning globally; dedicated tests
assert it fires by re-enabling it with ``warnings.catch_warnings`` /
``simplefilter("always")``.
"""

import sys
from unittest.mock import MagicMock

if "duckdb.functional" not in sys.modules:
    _mock = MagicMock()
    _mock.SPECIAL = "SPECIAL"
    sys.modules["duckdb.functional"] = _mock


def pytest_configure(config):
    import warnings

    from pysus.api.errors import PySUSWarning

    warnings.filterwarnings("ignore", category=PySUSWarning)
