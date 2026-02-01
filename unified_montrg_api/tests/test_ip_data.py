"""Tests for IP data endpoints and services."""

import os
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, date
from decimal import Decimal

# Test environment variable setting
os.environ.setdefault("IP04_DATABASE_URL", "mysql+aiomysql://test:test@localhost:3306/test_db")
os.environ.setdefault("IP12_DATABASE_URL", "mysql+aiomysql://test:test@localhost:3306/test_db")

# Prevent Oracle initialization error
if "CMMS_DATABASE_URL" in os.environ:
    del os.environ["CMMS_DATABASE_URL"]

from app.schemas.rtf_data import HmiData, HmiDataResponse, HmiDataMultiResponse


# Mock data helper function
def create_mock_row(seqno, rxdate, pvalue, pid):
    """Create mock row object (tuple form)"""
    row = MagicMock()
    row.SeqNo = seqno
    row.RxDate = rxdate
    row.Pvalue = pvalue
    row.PID = pid
    return row


@pytest.fixture
def mock_ip_data_rows():
    """IP data mock data"""
    return [
        create_mock_row(
            1,
            datetime(2024, 1, 15, 12, 0, 0),
            Decimal("123.456"),
            12345678901,
        ),
        create_mock_row(
            2,
            datetime(2024, 1, 15, 12, 1, 0),
            Decimal("234.567"),
            12345678901,
        ),
        create_mock_row(
            3,
            datetime(2024, 1, 15, 12, 2, 0),
            Decimal("345.678"),
            12345678901,
        ),
    ]


# ==================== Service function test ====================

@patch("app.services.ip_data.get_session")
@patch("app.services.ip_data.resolve_db_alias")
@pytest.mark.asyncio
async def test_fetch_ip_data_by_pid(mock_resolve_db_alias, mock_get_session, mock_ip_data_rows):
    """fetch_ip_data_by_pid service function test"""
    from app.services.ip_data import fetch_ip_data_by_pid

    # Mock setting
    mock_resolve_db_alias.return_value = "ip04"
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_ip_data_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_ip_data_by_pid(pid=12345678901, limit=500)

    # Validate
    assert result is not None
    assert isinstance(result, HmiDataResponse)
    assert result.PID == "12345678901"  # 11 digits, no padding needed
    assert len(result.values) == 3
    assert result.values[0].pvalue == Decimal("123.456")
    assert result.values[1].pvalue == Decimal("234.567")
    assert result.values[2].pvalue == Decimal("345.678")


@patch("app.services.ip_data.get_session")
@patch("app.services.ip_data.resolve_db_alias")
@pytest.mark.asyncio
async def test_fetch_ip_data_by_pid_empty_result(mock_resolve_db_alias, mock_get_session):
    """fetch_ip_data_by_pid - empty result test"""
    from app.services.ip_data import fetch_ip_data_by_pid

    # Mock setting
    mock_resolve_db_alias.return_value = "ip04"
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = []
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_ip_data_by_pid(pid=12345678901, limit=500)

    # Validate
    assert result is None


@patch("app.services.ip_data.get_session")
@patch("app.services.ip_data.resolve_db_alias")
@pytest.mark.asyncio
async def test_fetch_ip_data_by_pid_and_date_key(mock_resolve_db_alias, mock_get_session, mock_ip_data_rows):
    """fetch_ip_data_by_pid_and_date_key service function test"""
    from app.services.ip_data import fetch_ip_data_by_pid_and_date_key

    # Mock setting
    mock_resolve_db_alias.return_value = "ip04"
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_ip_data_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_ip_data_by_pid_and_date_key(pid=12345678901, limit=500, date_key="20240115")

    # Validate
    assert result is not None
    assert isinstance(result, HmiDataResponse)
    assert len(result.values) == 3


@patch("app.services.ip_data.get_session")
@patch("app.services.ip_data.resolve_db_alias")
@patch("app.services.ip_data.date")
@pytest.mark.asyncio
async def test_fetch_ip_data_latest(mock_date_module, mock_resolve_db_alias, mock_get_session):
    """fetch_ip_data_latest service function test - when date is today"""
    from app.services.ip_data import fetch_ip_data_latest

    # Mock setting
    mock_resolve_db_alias.return_value = "ip04"
    today = date.today()
    mock_date_module.today.return_value = today
    
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_row = create_mock_row(
        3,
        datetime(today.year, today.month, today.day, 12, 2, 0),
        Decimal("345.678"),
        12345678901,
    )
    mock_result.first.return_value = mock_row
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_ip_data_latest(pid=12345678901)

    # Validate
    assert result is not None
    assert isinstance(result, HmiDataResponse)
    assert result.PID == "12345678901"  # 11 digits, no padding needed
    assert len(result.values) == 1
    assert result.values[0].pvalue == Decimal("345.678")


@patch("app.services.ip_data.get_session")
@patch("app.services.ip_data.resolve_db_alias")
@pytest.mark.asyncio
async def test_fetch_ip_data_by_pid_and_range(mock_resolve_db_alias, mock_get_session, mock_ip_data_rows):
    """fetch_ip_data_by_pid_and_range service function test"""
    from app.services.ip_data import fetch_ip_data_by_pid_and_range

    # Mock setting
    mock_resolve_db_alias.return_value = "ip04"
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_ip_data_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_ip_data_by_pid_and_range(
        pid=12345678901,
        limit=500,
        start_key="20240115",
        end_key="20240116",
    )

    # Validate
    assert result is not None
    assert isinstance(result, HmiDataResponse)
    assert len(result.values) == 3


# ==================== API endpoint test ====================

@patch("app.api.v1.ip_data.fetch_ip_data_by_pid")
def test_get_ip_data_by_pid_endpoint(mock_fetch):
    """GET /api/v1/ip-data/{pid} endpoint test"""
    from app.main import app

    # Mock setting
    mock_response = HmiDataResponse(
        PID="12345678901",  # 11 digits, no padding needed
        values=[
            HmiData(rxdate="2024-01-15 12:00:00", pvalue=Decimal("123.456")),
            HmiData(rxdate="2024-01-15 12:01:00", pvalue=Decimal("234.567")),
        ],
    )
    mock_fetch.return_value = mock_response

    client = TestClient(app)
    response = client.get("/api/v1/ip-data/12345678901?limit=500")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["PID"] == "12345678901"
    assert len(data["values"]) == 2
    assert data["values"][0]["pvalue"] == "123.456"


@patch("app.api.v1.ip_data.fetch_ip_data_by_pid")
def test_get_ip_data_by_pid_not_found(mock_fetch):
    """GET /api/v1/ip-data/{pid} - no data test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = None

    client = TestClient(app)
    response = client.get("/api/v1/ip-data/12345678901")

    # Validate
    assert response.status_code == 404
    assert "No data found" in response.json()["detail"]


@patch("app.api.v1.ip_data.fetch_ip_data_by_pid_and_date_key")
def test_get_ip_data_by_date_key_endpoint(mock_fetch):
    """GET /api/v1/ip-data/{pid}/dates/{date_key} endpoint test"""
    from app.main import app

    # Mock setting
    mock_response = HmiDataResponse(
        PID="12345678901",  # 11 digits, no padding needed
        values=[
            HmiData(rxdate="2024-01-15 12:00:00", pvalue=Decimal("123.456")),
        ],
    )
    mock_fetch.return_value = mock_response

    client = TestClient(app)
    response = client.get("/api/v1/ip-data/12345678901/dates/20240115")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["PID"] == "12345678901"
    assert len(data["values"]) == 1




# ==================== Schema validation test ====================

def test_hmi_data_schema():
    """HmiData schema validation"""
    data = HmiData(
        rxdate="2024-01-15 12:00:00",
        pvalue=Decimal("123.456"),
    )

    assert data.rxdate == "2024-01-15 12:00:00"
    assert data.pvalue == Decimal("123.456")


def test_hmi_data_response_schema():
    """HmiDataResponse schema validation"""
    response = HmiDataResponse(
        PID="12345678901",  # 11 digits, no padding needed
        values=[
            HmiData(rxdate="2024-01-15 12:00:00", pvalue=Decimal("123.456")),
            HmiData(rxdate="2024-01-15 12:01:00", pvalue=Decimal("234.567")),
        ],
    )

    assert response.PID == "12345678901"
    assert len(response.values) == 2
    assert response.values[0].pvalue == Decimal("123.456")


@patch("app.api.v1.ip_data.fetch_ip_data_by_pid")
def test_get_ip_data_by_pids_endpoint(mock_fetch):
    """GET /api/v1/ip-data?pid=...&pid=... multiple PID endpoint test"""
    from app.main import app

    # Mock setting
    mock_fetch.side_effect = [
        HmiDataResponse(
            PID="12345678901",
            values=[
                HmiData(rxdate="2024-01-15 12:00:00", pvalue=Decimal("123.456")),
            ],
        ),
        HmiDataResponse(
            PID="98765432101",
            values=[
                HmiData(rxdate="2024-01-15 12:00:00", pvalue=Decimal("456.789")),
            ],
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/ip-data?pid=12345678901&pid=98765432101&limit=500")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
    assert len(data["data"]) == 2
    assert data["data"][0]["PID"] == "12345678901"
    assert data["data"][1]["PID"] == "98765432101"


@patch("app.api.v1.ip_data.fetch_ip_data_latest_by_pids")
def test_get_ip_data_latest_by_pids_endpoint(mock_fetch):
    """GET /api/v1/ip-data/latest?pid=...&pid=... multiple PID latest data endpoint test"""
    from app.main import app

    # Mock setting - fetch_ip_data_latest_by_pids returns List[HmiDataResponse]
    mock_fetch.return_value = [
        HmiDataResponse(
            PID="12345678901",
            values=[
                HmiData(rxdate="2024-01-15 12:02:00", pvalue=Decimal("345.678")),
            ],
        ),
        HmiDataResponse(
            PID="98765432101",
            values=[
                HmiData(rxdate="2024-01-15 12:02:00", pvalue=Decimal("678.901")),
            ],
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/ip-data/latest", params={"pid": [12345678901, 98765432101]})

    # Validate
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["total"] == 2
    assert len(data["data"]) == 2
    assert data["data"][0]["PID"] == "12345678901"
    assert data["data"][1]["PID"] == "98765432101"


def test_hmi_data_multi_response_schema():
    """HmiDataMultiResponse schema validation"""
    response = HmiDataMultiResponse(
        data=[
            HmiDataResponse(
                PID="12345678901",
                values=[
                    HmiData(rxdate="2024-01-15 12:00:00", pvalue=Decimal("123.456")),
                ],
            ),
            HmiDataResponse(
                PID="98765432101",
                values=[
                    HmiData(rxdate="2024-01-15 12:00:00", pvalue=Decimal("456.789")),
                ],
            ),
        ],
        total=2,
    )

    assert response.total == 2
    assert len(response.data) == 2
    assert response.data[0].PID == "12345678901"
    assert response.data[1].PID == "98765432101"

