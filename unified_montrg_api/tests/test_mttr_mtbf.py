"""Tests for MTTR/MTBF service."""

import os
import pytest
from unittest.mock import patch, MagicMock
from datetime import date

# Test environment variable setting
# Prevent Oracle initialization error
if "CMMS_DATABASE_URL" in os.environ:
    del os.environ["CMMS_DATABASE_URL"]

from app.schemas.mttr_mtbf import MttrMtbfRecord, MttrMtbfResponse


# Mock data helper function
def create_mock_row(*values):
    """Create mock row object (tuple form)"""
    return tuple(values)


@pytest.fixture
def mock_mttr_mtbf_rows():
    """MTTR/MTBF mock data"""
    return [
        create_mock_row(
            "OS",
            "20240101",  # VARCHAR2(8) format
            30.0,
            25.0,
            1440.0,
            1200.0,
        ),
        create_mock_row(
            "IP",
            "20240101",
            35.0,
            30.0,
            1200.0,
            1000.0,
        ),
    ]


# ==================== helper function test ====================

def test_get_first_day_of_month_str():
    """get_first_day_of_month_str helper function test"""
    from app.services.mttr_mtbf import get_first_day_of_month_str

    # Normal case
    assert get_first_day_of_month_str(date(2024, 1, 15)) == "20240101"
    assert get_first_day_of_month_str(date(2024, 2, 28)) == "20240201"
    assert get_first_day_of_month_str(date(2024, 12, 31)) == "20241201"
    
    # Already first day of month
    assert get_first_day_of_month_str(date(2024, 1, 1)) == "20240101"


# ==================== Service function test ====================

@patch("app.services.mttr_mtbf.get_sync_engine")
@pytest.mark.asyncio
async def test_fetch_mttr_mtbf_default(mock_get_sync_engine, mock_mttr_mtbf_rows):
    """fetch_mttr_mtbf - default query test (current month)"""
    from app.services.mttr_mtbf import fetch_mttr_mtbf

    # Mock engine setting
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchall.return_value = mock_mttr_mtbf_rows
    mock_conn.execute.return_value = mock_result
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
    mock_get_sync_engine.return_value = mock_engine

    # Call service function
    result = await fetch_mttr_mtbf()

    # Validate
    assert len(result) == 2
    assert isinstance(result[0], MttrMtbfRecord)
    assert result[0].process == "OS"
    assert result[0].date == date(2024, 1, 1)  # String converted to date
    assert result[0].mttr_target == 30.0
    assert result[0].mttr == 25.0
    assert result[0].mtbf_target == 1440.0
    assert result[0].mtbf == 1200.0


@patch("app.services.mttr_mtbf.get_sync_engine")
@pytest.mark.asyncio
async def test_fetch_mttr_mtbf_with_date(mock_get_sync_engine, mock_mttr_mtbf_rows):
    """fetch_mttr_mtbf - date filter test"""
    from app.services.mttr_mtbf import fetch_mttr_mtbf

    # Mock engine setting
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchall.return_value = [mock_mttr_mtbf_rows[0]]
    mock_conn.execute.return_value = mock_result
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
    mock_get_sync_engine.return_value = mock_engine

    # Call service function
    result = await fetch_mttr_mtbf(date=date(2024, 1, 15))

    # Validate
    assert len(result) == 1
    assert result[0].date == date(2024, 1, 1)  # Converted to first day


@patch("app.services.mttr_mtbf.get_sync_engine")
@pytest.mark.asyncio
async def test_fetch_mttr_mtbf_with_process(mock_get_sync_engine, mock_mttr_mtbf_rows):
    """fetch_mttr_mtbf - process filter test"""
    from app.services.mttr_mtbf import fetch_mttr_mtbf

    # Mock engine setting
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    # Return only OS process
    mock_result.fetchall.return_value = [mock_mttr_mtbf_rows[0]]
    mock_conn.execute.return_value = mock_result
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
    mock_get_sync_engine.return_value = mock_engine

    # Call service function
    result = await fetch_mttr_mtbf(process="OS")

    # Validate
    assert len(result) == 1
    assert result[0].process == "OS"


@patch("app.services.mttr_mtbf.get_sync_engine")
@pytest.mark.asyncio
async def test_fetch_mttr_mtbf_with_invalid_process(mock_get_sync_engine):
    """fetch_mttr_mtbf - invalid process filter test"""
    from app.services.mttr_mtbf import fetch_mttr_mtbf

    # Mock engine setting
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    mock_conn.execute.return_value = mock_result
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
    mock_get_sync_engine.return_value = mock_engine

    # Call service function
    result = await fetch_mttr_mtbf(process="INVALID")

    # Validate
    assert len(result) == 0


@patch("app.services.mttr_mtbf.get_sync_engine")
@pytest.mark.asyncio
async def test_fetch_mttr_mtbf_empty_result(mock_get_sync_engine):
    """fetch_mttr_mtbf - empty result test"""
    from app.services.mttr_mtbf import fetch_mttr_mtbf

    # Mock engine setting
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    mock_conn.execute.return_value = mock_result
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
    mock_get_sync_engine.return_value = mock_engine

    # Call service function
    result = await fetch_mttr_mtbf()

    # Validate
    assert len(result) == 0


@patch("app.services.mttr_mtbf.get_sync_engine")
@pytest.mark.asyncio
async def test_fetch_mttr_mtbf_invalid_date_format(mock_get_sync_engine):
    """fetch_mttr_mtbf - invalid date format handling test"""
    from app.services.mttr_mtbf import fetch_mttr_mtbf

    # Mock engine setting
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    # Invalid date format
    mock_result.fetchall.return_value = [
        create_mock_row(
            "OS",
            "INVALID",  # Invalid date format
            30.0,
            25.0,
            1440.0,
            1200.0,
        ),
    ]
    mock_conn.execute.return_value = mock_result
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
    mock_get_sync_engine.return_value = mock_engine

    # Call service function
    result = await fetch_mttr_mtbf()

    # Validate
    assert len(result) == 1
    assert result[0].date is None  # Invalid format is handled as None


# ==================== Schema validation test ====================

def test_mttr_mtbf_record_schema():
    """MttrMtbfRecord schema validation"""
    record = MttrMtbfRecord(
        process="OS",
        date=date(2024, 1, 1),
        mttr_target=30.0,
        mttr=25.0,
        mtbf_target=1440.0,
        mtbf=1200.0,
    )

    assert record.process == "OS"
    assert record.mttr_target == 30.0
    assert record.mtbf == 1200.0


def test_mttr_mtbf_response_schema():
    """MttrMtbfResponse schema validation"""
    records = [
        MttrMtbfRecord(
            process="OS",
            date=date(2024, 1, 1),
            mttr_target=30.0,
            mttr=25.0,
            mtbf_target=1440.0,
            mtbf=1200.0,
        ),
    ]
    response = MttrMtbfResponse(data=records, total=1)

    assert response.total == 1
    assert len(response.data) == 1
    assert response.data[0].process == "OS"

