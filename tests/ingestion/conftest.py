# tests/ingestion/conftest.py
#
# Shared test configuration for ingestion unit tests.
#
# IMPORTANT: The os.environ.setdefault calls below MUST be at module level,
# not inside fixtures. Pydantic Settings reads env vars when `settings = Settings()`
# runs at import time in ingestion/config.py. Module-level conftest code runs
# before pytest collects (and imports) test files, ensuring env vars are present
# when the ingestion modules are first imported.

import os
from unittest.mock import MagicMock

import pytest

# ── Required env vars — set before any ingestion module is imported ────────────
os.environ.setdefault("SNOWFLAKE_ACCOUNT", "testaccount.us-east-1")
os.environ.setdefault("SNOWFLAKE_USER", "test_user")
os.environ.setdefault("SNOWFLAKE_PASSWORD", "test_password")
os.environ.setdefault("CENSUS_API_KEY", "test_census_key")
os.environ.setdefault("FRED_API_KEY", "test_fred_key")

# ── Shared Snowflake connection mock ───────────────────────────────────────────

@pytest.fixture
def mock_snowflake_cursor():
    """
    A MagicMock cursor ready to use inside patched Snowflake connections.

    MagicMock handles the context manager protocol automatically:
        with get_snowflake_conn() as conn:   # __enter__ returns another MagicMock
            cursor = conn.cursor()            # returns MagicMock cursor
            cursor.execute(...)              # no-op MagicMock call

    Usage in tests:
        with patch("ingestion.utils.get_snowflake_conn"):
            result = write_to_snowflake(rows, "TABLE", ["col"])
    """
    return MagicMock()