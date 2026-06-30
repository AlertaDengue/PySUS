"""pytest configuration - mocks duckdb.functional before any other imports."""

import sys
from unittest.mock import MagicMock

if "duckdb.functional" not in sys.modules:
    _mock = MagicMock()
    _mock.SPECIAL = "SPECIAL"
    sys.modules["duckdb.functional"] = _mock
