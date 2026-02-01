"""Tests for IP Rollgap service."""

import os
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime

# Test environment variable setting
os.environ.setdefault("CKP_IP_DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test_db")

# Prevent Oracle initialization error - remove CMMS_DATABASE_URL to skip Oracle initialization
# ip_rollgap test only uses PostgreSQL, so prevent Oracle initialization
if "CMMS_DATABASE_URL" in os.environ:
    del os.environ["CMMS_DATABASE_URL"]

from app.schemas.ip_rollgap import IpRollgapRecord, IpRollgapResponse


# Mock data helper function
def create_mock_row(*values):
    """Create mock row object (tuple form)"""
    return tuple(values)


@pytest.fixture
def mock_ip_rollgap_rows():
    """Mock data for IP Rollgap (returns sensor_id)"""
    return [
        create_mock_row(
            "IPRIOT-A201",  # sensor_id -> mapped to "Roll A"
            datetime(2024, 1, 15, 12, 0, 0),
            1.25,
            1.30,
        ),
        create_mock_row(
            "IPRIOT-A202",  # sensor_id -> mapped to "Roll B"
            datetime(2024, 1, 15, 12, 0, 0),
            1.35,
            1.40,
        ),
        create_mock_row(
            "IPRIOT-A203",  # sensor_id -> mapped to "Roll C"
            datetime(2024, 1, 15, 12, 0, 0),
            1.45,
            1.50,
        ),
        create_mock_row(
            "IPRIOT-A204",  # sensor_id -> mapped to "Roll D"
            datetime(2024, 1, 15, 12, 0, 0),
            1.55,
            1.60,
        ),
    ]


# ==================== Service function test ====================

@patch("app.services.ip_rollgap.get_session")
@pytest.mark.asyncio
async def test_fetch_ip_rollgap(mock_get_session, mock_ip_rollgap_rows):
    """fetch_ip_rollgap service function test"""
    from app.services.ip_rollgap import fetch_ip_rollgap

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_ip_rollgap_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_ip_rollgap()

    # Validate
    assert len(result) == 4
    assert isinstance(result[0], IpRollgapRecord)
    assert result[0].roll_name == "Roll A"
    assert result[0].gap_left == 1.25
    assert result[0].gap_right == 1.30
    assert result[1].roll_name == "Roll B"
    assert result[2].roll_name == "Roll C"
    assert result[3].roll_name == "Roll D"


@patch("app.services.ip_rollgap.get_session")
@pytest.mark.asyncio
async def test_fetch_ip_rollgap_empty_result(mock_get_session):
    """fetch_ip_rollgap - empty result test"""
    from app.services.ip_rollgap import fetch_ip_rollgap

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = []
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_ip_rollgap()

    # Validate
    assert len(result) == 0


@patch("app.services.ip_rollgap.get_session")
@pytest.mark.asyncio
async def test_fetch_ip_rollgap_with_none_values(mock_get_session):
    """fetch_ip_rollgap - None value handling test"""
    from app.services.ip_rollgap import fetch_ip_rollgap

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = [
        create_mock_row(
            "IPRIOT-A201",  # sensor_id -> mapped to "Roll A"
            datetime(2024, 1, 15, 12, 0, 0),
            None,
            None,
        ),
    ]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_ip_rollgap()

    # Validate
    assert len(result) == 1
    assert result[0].gap_left is None
    assert result[0].gap_right is None


# ==================== Schema validation test ====================

def test_ip_rollgap_record_schema():
    """IpRollgapRecord schema validation"""
    record = IpRollgapRecord(
        roll_name="Roll A",
        capture_dt=datetime(2024, 1, 15, 12, 0, 0),
        gap_left=1.25,
        gap_right=1.30,
    )

    assert record.roll_name == "Roll A"
    assert record.gap_left == 1.25
    assert record.gap_right == 1.30


def test_ip_rollgap_response_schema():
    """IpRollgapResponse schema validation"""
    records = [
        IpRollgapRecord(
            roll_name="Roll A",
            capture_dt=datetime(2024, 1, 15, 12, 0, 0),
            gap_left=1.25,
            gap_right=1.30,
        ),
    ]
    response = IpRollgapResponse(data=records, total=1)

    assert response.total == 1
    assert len(response.data) == 1
    assert response.data[0].roll_name == "Roll A"

