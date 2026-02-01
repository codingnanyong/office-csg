"""Pytest configuration and fixtures for unified_montrg tests."""

import os
import pytest

# Set environment variables at the module level (must be executed before import)
# Dummy database URL setting (actual connection is not made, all queries are mocked)
os.environ.setdefault("MONTRG_DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test_db")

# Oracle (CMMS) database URL setting
# If the actual value is in the environment variable, use it, otherwise use the default value
# To test actual Oracle connection, set CMMS_DATABASE_URL in the environment variable
if "CMMS_DATABASE_URL" not in os.environ:
    # Default Oracle URL (actual URL needs to be set in the environment variable for actual connection test)
    os.environ.setdefault(
        "CMMS_DATABASE_URL",
        "oracle+oracledb://{CMMS_USER}:{CMMS_PASSWORD}@{CMMS_HOST}:1521/{CMMS_SERVICE}"
    )

# Oracle client initialization skip for environment variable setting
# To run in test environment even if the Oracle library is not available
os.environ.setdefault("ORACLE_CLIENT_LIB", "")


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Test environment setup (run only once before all tests)
    
    Important:
    - Actual database connection information is not used
    - Tests mock _execute_query, so actual DB connection does not occur
    - This URL is a dummy value to prevent RuntimeError in app.core.database
    """
    yield
    # Clean up after tests (if needed)
    pass

