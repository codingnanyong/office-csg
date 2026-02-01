"""Tests for Chiller Status endpoints and services."""

import os
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime

# Test environment variable setting
# chiller only uses PostgreSQL (10.10.49.199:5432/EDGE_CHILLER)
os.environ.setdefault("CKP_CHILLER_DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test_db")

# chiller test does not use Oracle, so CMMS_DATABASE_URL is removed to skip Oracle initialization
# database.py checks "cmms" in settings.database_urls to prevent passing
if "CMMS_DATABASE_URL" in os.environ:
    del os.environ["CMMS_DATABASE_URL"]

# chiller_status only tests, so app.main is not imported and tested directly
# To avoid import errors from other modules, only necessary imports are made
from app.schemas.chiller_status import (
    ChillerStatusRecord,
    ChillerStatusResponse,
    ChillerRunningStatusRecord,
    ChillerRunningStatusResponse,
    ChillerAlarmStatusRecord,
    ChillerAlarmStatusResponse,
    ChillerStatusHistoryRecord,
    ChillerStatusHistoryItem,
    ChillerStatusHistoryResponse,
)

# app.main is not imported, so TestClient is created as needed for each test


# Mock data helper function
def create_mock_row(*values):
    """Create mock row object (tuple form) - return actual tuple"""
    # Return actual tuple to allow access to row[0], row[1], etc.
    return tuple(values)


@pytest.fixture
def mock_chiller_status_rows():
    """Mock data for Chiller Status"""
    return [
        create_mock_row(
            "Chiller 01",
            datetime(2024, 1, 1, 12, 0, 0),
            25.5,
            30.2,
            20.0,
            35.1,
            36.2,
            37.3,
            38.4,
        ),
        create_mock_row(
            "Chiller 02",
            datetime(2024, 1, 1, 12, 0, 0),
            26.0,
            31.0,
            21.0,
            36.0,
            37.0,
            38.0,
            39.0,
        ),
    ]


@pytest.fixture
def mock_chiller_running_status_rows():
    """Mock data for Chiller Running Status"""
    return [
        create_mock_row("CKP001", "1"),
        create_mock_row("CKP002", "0"),
        create_mock_row("CKP003", "1"),
    ]


@pytest.fixture
def mock_chiller_alarm_status_rows():
    """Mock data for Chiller Alarm Status"""
    return [
        create_mock_row(
            "Flow_SW_Alarm",
            2,
            [
                {"device_id": "CKP001", "device_name": "Chiller 01", "upd_dt": "2024-01-01T12:00:00"},
                {"device_id": "CKP003", "device_name": "Chiller 03", "upd_dt": "2024-01-01T12:00:00"},
            ],
        ),
        create_mock_row(
            "#1_Comp_OCR_Alarm",
            1,
            [
                {"device_id": "CKP001", "device_name": "Chiller 01", "upd_dt": "2024-01-01T12:00:00"},
            ],
        ),
        create_mock_row("#2_Comp_OCR_Alarm", 0, []),
    ]


@pytest.fixture
def mock_chiller_status_history_rows():
    """Mock data for Chiller Status History (before grouping by device_id)"""
    return [
        # data for device_id: CKP001
        create_mock_row(
            "CKP001",
            datetime(2024, 1, 1, 9, 0, 0),  # bucket_time
            10.5,  # water_in_temp
            15.2,  # water_out_temp
            20.0,  # sv_temp
            25.1,  # discharge_temp_1
            26.2,  # discharge_temp_2
            27.3,  # discharge_temp_3
            28.4,  # discharge_temp_4
        ),
        create_mock_row(
            "CKP001",
            datetime(2024, 1, 1, 9, 10, 0),
            11.0,
            16.0,
            21.0,
            26.0,
            27.0,
            28.0,
            29.0,
        ),
        # data for device_id: CKP002
        create_mock_row(
            "CKP002",
            datetime(2024, 1, 1, 9, 0, 0),
            12.5,
            17.2,
            22.0,
            30.1,
            31.2,
            32.3,
            33.4,
        ),
        create_mock_row(
            "CKP002",
            datetime(2024, 1, 1, 9, 10, 0),
            13.0,
            18.0,
            23.0,
            31.0,
            32.0,
            33.0,
            34.0,
        ),
    ]

# ==================== Service function test ====================

@patch("app.services.chiller_status.get_session")
@pytest.mark.asyncio
async def test_fetch_chiller_status_service(mock_get_session, mock_chiller_status_rows):
    """fetch_chiller_status service function test"""
    from app.services.chiller_status import fetch_chiller_status

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_chiller_status_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_chiller_status()

    # Validate
    assert len(result) == 2
    assert isinstance(result[0], ChillerStatusRecord)
    assert result[0].chiller_name == "Chiller 01"
    assert result[0].water_in_temp == 25.5


@patch("app.services.chiller_status.get_session")
@pytest.mark.asyncio
async def test_fetch_chiller_running_status_service(mock_get_session, mock_chiller_running_status_rows):
    """fetch_chiller_running_status service function test"""
    from app.services.chiller_status import fetch_chiller_running_status

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_chiller_running_status_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_chiller_running_status()

    # Validate
    assert len(result) == 3
    assert isinstance(result[0], ChillerRunningStatusRecord)
    assert result[0].device_id == "CKP001"
    assert result[0].running == "1"


@patch("app.services.chiller_status.get_session")
@pytest.mark.asyncio
async def test_fetch_chiller_alarm_status_service(mock_get_session, mock_chiller_alarm_status_rows):
    """fetch_chiller_alarm_status service function test"""
    from app.services.chiller_status import fetch_chiller_alarm_status

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_chiller_alarm_status_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_chiller_alarm_status()

    # Validate
    assert len(result) == 3
    assert isinstance(result[0], ChillerAlarmStatusRecord)
    assert result[0].alarm_name == "Flow_SW_Alarm"
    assert result[0].alarm_count == 2
    assert len(result[0].device_info) == 2
    assert result[2].alarm_count == 0
    assert len(result[2].device_info) == 0


# ==================== Helper function test ====================

def test_safe_float():
    """_safe_float helper function test"""
    from app.services.chiller_status import _safe_float

    # Normal float value
    assert _safe_float("25.5") == 25.5
    assert _safe_float(25.5) == 25.5
    assert _safe_float(25) == 25.0

    # None value
    assert _safe_float(None) is None

    # Invalid value
    assert _safe_float("invalid") is None
    assert _safe_float([]) is None


def test_build_alarm_map_values_sql():
    """_build_alarm_map_values_sql helper function test"""
    from app.services.chiller_status import _build_alarm_map_values_sql, ALARM_MAP

    sql = _build_alarm_map_values_sql()

    # Validate if SQL is correctly generated
    assert "SELECT * FROM (VALUES" in sql
    assert "AS a(bit_pos, alarm_name)" in sql

    # Validate if all alarms are included
    for bit_pos, alarm_name in ALARM_MAP:
        assert f"({bit_pos}, '{alarm_name}')" in sql


# ==================== Schema validation test ====================

def test_chiller_status_record_schema():
    """ChillerStatusRecord schema validation"""
    record = ChillerStatusRecord(
        chiller_name="Chiller 01",
        upd_dt=datetime(2024, 1, 1, 12, 0, 0),
        water_in_temp=25.5,
        water_out_temp=30.2,
        external_temp=20.0,
        discharge_temp_1=35.1,
        discharge_temp_2=36.2,
        discharge_temp_3=37.3,
        discharge_temp_4=38.4,
    )

    assert record.chiller_name == "Chiller 01"
    assert record.water_in_temp == 25.5
    assert record.discharge_temp_4 == 38.4


def test_chiller_running_status_record_schema():
    """ChillerRunningStatusRecord schema validation"""
    record = ChillerRunningStatusRecord(
        device_id="CKP001",
        running="1",
    )

    assert record.device_id == "CKP001"
    assert record.running == "1"


def test_chiller_alarm_status_record_schema():
    """ChillerAlarmStatusRecord schema validation"""
    record = ChillerAlarmStatusRecord(
        alarm_name="Flow_SW_Alarm",
        alarm_count=2,
        device_info=[
            {"device_id": "CKP001", "device_name": "Chiller 01", "upd_dt": "2024-01-01T12:00:00"},
        ],
    )

    assert record.alarm_name == "Flow_SW_Alarm"
    assert record.alarm_count == 2
    assert len(record.device_info) == 1
    assert record.device_info[0]["device_id"] == "CKP001"


def test_chiller_status_response_schema():
    """ChillerStatusResponse schema validation"""
    records = [
        ChillerStatusRecord(
            chiller_name="Chiller 01",
            upd_dt=datetime(2024, 1, 1, 12, 0, 0),
            water_in_temp=25.5,
        ),
    ]
    response = ChillerStatusResponse(data=records, total=1)

    assert response.total == 1
    assert len(response.data) == 1
    assert response.data[0].chiller_name == "Chiller 01"


# ==================== History service function test ====================

@patch("app.services.chiller_status.get_session")
@pytest.mark.asyncio
async def test_fetch_chiller_status_history_service(mock_get_session, mock_chiller_status_history_rows):
    """fetch_chiller_status_history service function test"""
    from app.services.chiller_status import fetch_chiller_status_history

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_chiller_status_history_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_chiller_status_history(hours=6)

    # Validate - grouped by device_id
    assert len(result) == 2
    assert isinstance(result[0], ChillerStatusHistoryRecord)
    assert result[0].device_id == "Chiller 01"  # _format_chiller_name converted
    assert len(result[0].history) == 2
    assert isinstance(result[0].history[0], ChillerStatusHistoryItem)
    assert result[0].history[0].bucket_time == datetime(2024, 1, 1, 9, 0, 0)
    assert result[0].history[0].water_in_temp == 10.5
    assert result[0].history[0].water_out_temp == 15.2
    
    assert result[1].device_id == "Chiller 02"  # _format_chiller_name converted
    assert len(result[1].history) == 2
    assert result[1].history[0].water_in_temp == 12.5


@patch("app.services.chiller_status.get_session")
@pytest.mark.asyncio
async def test_fetch_chiller_status_history_with_device_id(mock_get_session, mock_chiller_status_history_rows):
    """fetch_chiller_status_history - filter by specific device_id test"""
    from app.services.chiller_status import fetch_chiller_status_history

    # Mock session setting - return only CKP001
    mock_session = AsyncMock()
    mock_result = MagicMock()
    ckp001_rows = [row for row in mock_chiller_status_history_rows if row[0] == "CKP001"]
    mock_result.all.return_value = ckp001_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_chiller_status_history(device_id="CKP001", hours=6)

    # Validate
    assert len(result) == 1
    assert result[0].device_id == "Chiller 01"  # _format_chiller_name converted
    assert len(result[0].history) == 2


@patch("app.services.chiller_status.get_session")
@pytest.mark.asyncio
async def test_fetch_chiller_status_history_by_range_service(mock_get_session, mock_chiller_status_history_rows):
    """fetch_chiller_status_history_by_range service function test"""
    from app.services.chiller_status import fetch_chiller_status_history_by_range

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_chiller_status_history_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    start_dt = datetime(2024, 1, 1, 8, 0, 0)
    end_dt = datetime(2024, 1, 1, 10, 0, 0)
    result = await fetch_chiller_status_history_by_range(start_dt=start_dt, end_dt=end_dt)

    # Validate
    assert len(result) == 2
    assert isinstance(result[0], ChillerStatusHistoryRecord)
    assert result[0].device_id == "Chiller 01"  # _format_chiller_name converted
    assert len(result[0].history) == 2


@patch("app.services.chiller_status.get_session")
@pytest.mark.asyncio
async def test_fetch_chiller_status_history_by_range_with_device_id(mock_get_session, mock_chiller_status_history_rows):
    """fetch_chiller_status_history_by_range - filter by specific device_id test"""
    from app.services.chiller_status import fetch_chiller_status_history_by_range

    # Mock session setting - return only CKP002
    mock_session = AsyncMock()
    mock_result = MagicMock()
    ckp002_rows = [row for row in mock_chiller_status_history_rows if row[0] == "CKP002"]
    mock_result.all.return_value = ckp002_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    start_dt = datetime(2024, 1, 1, 8, 0, 0)
    end_dt = datetime(2024, 1, 1, 10, 0, 0)
    result = await fetch_chiller_status_history_by_range(device_id="CKP002", start_dt=start_dt, end_dt=end_dt)

    # Validate
    assert len(result) == 1
    assert result[0].device_id == "Chiller 02"  # _format_chiller_name converted
    assert len(result[0].history) == 2


# ==================== History schema validation test ====================

def test_chiller_status_history_item_schema():
    """ChillerStatusHistoryItem schema validation"""
    item = ChillerStatusHistoryItem(
        bucket_time=datetime(2024, 1, 1, 9, 0, 0),
        water_in_temp=10.5,
        water_out_temp=15.2,
        sv_temp=20.0,
        discharge_temp_1=25.1,
        discharge_temp_2=26.2,
        discharge_temp_3=27.3,
        discharge_temp_4=28.4,
    )

    assert item.bucket_time == datetime(2024, 1, 1, 9, 0, 0)
    assert item.water_in_temp == 10.5
    assert item.water_out_temp == 15.2
    assert item.sv_temp == 20.0
    assert item.discharge_temp_4 == 28.4


def test_chiller_status_history_record_schema():
    """ChillerStatusHistoryRecord schema validation"""
    history_items = [
        ChillerStatusHistoryItem(
            bucket_time=datetime(2024, 1, 1, 9, 0, 0),
            water_in_temp=10.5,
            water_out_temp=15.2,
        ),
        ChillerStatusHistoryItem(
            bucket_time=datetime(2024, 1, 1, 9, 10, 0),
            water_in_temp=11.0,
            water_out_temp=16.0,
        ),
    ]
    
    record = ChillerStatusHistoryRecord(
        device_id="CKP001",
        history=history_items,
    )

    assert record.device_id == "CKP001"
    assert len(record.history) == 2
    assert record.history[0].bucket_time == datetime(2024, 1, 1, 9, 0, 0)
    assert record.history[1].bucket_time == datetime(2024, 1, 1, 9, 10, 0)


def test_chiller_status_history_response_schema():
    """ChillerStatusHistoryResponse schema validation"""
    records = [
        ChillerStatusHistoryRecord(
            device_id="CKP001",
            history=[
                ChillerStatusHistoryItem(
                    bucket_time=datetime(2024, 1, 1, 9, 0, 0),
                    water_in_temp=10.5,
                ),
            ],
        ),
    ]
    response = ChillerStatusHistoryResponse(data=records, total=1)

    assert response.total == 1
    assert len(response.data) == 1
    assert response.data[0].device_id == "CKP001"
    assert len(response.data[0].history) == 1


# ==================== API endpoint test ====================

@patch("app.api.v1.chiller_status.fetch_chiller_status_history")
def test_get_chiller_status_history_endpoint(mock_fetch):
    """GET /api/v1/chiller-status/history endpoint test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        ChillerStatusHistoryRecord(
            device_id="CKP001",
            history=[
                ChillerStatusHistoryItem(
                    bucket_time=datetime(2024, 1, 1, 9, 0, 0),
                    water_in_temp=10.5,
                    water_out_temp=15.2,
                ),
            ],
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/chiller-status/history?hours=6")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert len(data["data"]) == 1
    assert data["data"][0]["device_id"] == "CKP001"
    assert len(data["data"][0]["history"]) == 1
    assert data["data"][0]["history"][0]["water_in_temp"] == 10.5


@patch("app.api.v1.chiller_status.fetch_chiller_status_history")
def test_get_chiller_status_history_endpoint_with_device_id(mock_fetch):
    """GET /api/v1/chiller-status/history?device_id=... endpoint test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        ChillerStatusHistoryRecord(
            device_id="CKP001",
            history=[
                ChillerStatusHistoryItem(
                    bucket_time=datetime(2024, 1, 1, 9, 0, 0),
                    water_in_temp=10.5,
                ),
            ],
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/chiller-status/history?device_id=CKP001&hours=6")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["data"][0]["device_id"] == "CKP001"


@patch("app.api.v1.chiller_status.resolve_date_range")
@patch("app.api.v1.chiller_status.fetch_chiller_status_history_by_range")
def test_get_chiller_status_history_by_range_endpoint(mock_fetch, mock_resolve_date_range):
    """GET /api/v1/chiller-status/history/range endpoint test"""
    from app.main import app

    # Mock setting
    # resolve_date_range returns datetime object
    mock_resolve_date_range.return_value = (
        datetime(2024, 1, 1, 8, 0, 0),
        datetime(2024, 1, 1, 10, 0, 0),
    )
    
    mock_fetch.return_value = [
        ChillerStatusHistoryRecord(
            device_id="Chiller 01",  # _format_chiller_name converted
            history=[
                ChillerStatusHistoryItem(
                    bucket_time=datetime(2024, 1, 1, 9, 0, 0),
                    water_in_temp=10.5,
                ),
            ],
        ),
    ]

    client = TestClient(app)
    # Supported date formats: yyyyMMdd HH:MM:SS
    start_dt = "20240101 08:00:00"
    end_dt = "20240101 10:00:00"
    response = client.get(f"/api/v1/chiller-status/history/range?start_dt={start_dt}&end_dt={end_dt}")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["data"][0]["device_id"] == "Chiller 01"  # _format_chiller_name converted
    
    # resolve_date_range called with correct arguments
    mock_resolve_date_range.assert_called_once_with(start_dt, end_dt)
    mock_fetch.assert_called_once_with(
        device_id=None,
        start_dt=datetime(2024, 1, 1, 8, 0, 0),
        end_dt=datetime(2024, 1, 1, 10, 0, 0),
    )


@patch("app.api.v1.chiller_status.fetch_chiller_status_history_by_range")
def test_get_chiller_status_history_by_range_endpoint_missing_params(mock_fetch):
    """GET /api/v1/chiller-status/history/range - missing parameters test"""
    from app.main import app

    client = TestClient(app)
    response = client.get("/api/v1/chiller-status/history/range")

    # Validate - start_dt and end_dt are both missing, return 400 error
    assert response.status_code == 400
    assert "start_dt or end_dt" in response.json()["detail"]

