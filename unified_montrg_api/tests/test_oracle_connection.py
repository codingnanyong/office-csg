"""Tests for Oracle database connection (CMMS)."""

import os
import pytest
from sqlalchemy import text

# Prevent Oracle initialization error
# Calling available_databases() at module level attempts Oracle initialization
# So changed to check inside function

def _is_cmms_available():
    """Check if CMMS database is configured (called inside function)"""
    try:
        from app.core.database import available_databases
        return "cmms" in available_databases()
    except Exception:
        return False


@pytest.mark.asyncio
@pytest.mark.skipif(not _is_cmms_available(), reason="CMMS database not configured (CMMS_DATABASE_URL not set)")
async def test_cmms_database_available():
    """Test that CMMS database is configured."""
    from app.core.database import available_databases
    databases = available_databases()
    assert "cmms" in databases, f"CMMS database not found. Available databases: {databases}"


@pytest.mark.asyncio
@pytest.mark.skipif(not _is_cmms_available(), reason="CMMS database not configured (CMMS_DATABASE_URL not set)")
async def test_cmms_connection():
    """Test basic connection to CMMS Oracle database."""
    from app.core.database import get_session
    try:
        async with get_session(db_alias="cmms") as session:
            # Test basic query
            result = await session.execute(text("SELECT 1 FROM DUAL"))
            row = result.scalar()
            assert row == 1, "Query should return 1"
    except Exception as e:
        pytest.fail(f"Failed to connect to CMMS database: {str(e)}")
