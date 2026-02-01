"""Tests for Banbury Anomaly Detection endpoints and services."""

import os
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime
from zoneinfo import ZoneInfo

# Test environment variable setting
os.environ.setdefault("MONTRG_DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test_db")

# Oracle initialization error prevention
if "CMMS_DATABASE_URL" in os.environ:
    del os.environ["CMMS_DATABASE_URL"]

from app.schemas.banbury_anomaly_detection import (
    BanburyAnomalyResultRecord,
    BanburyAnomalyResultResponse,
)


# Mock data helper function
def create_mock_banbury_record(
    no: str = "TEST001",
    shift: int = 1,
    cycle_start: datetime = None,
    cycle_end: datetime = None,
    mode: str = "normal",
    mix_duration_sec: float = 120.5,
    max_temp: float = 150.25,
    is_3_stage: bool = False,
    is_anomaly: bool = False,
    anomaly_prob: float = 0.1234,
    filtered_num: int = 10,
    peak_count: int = 5,
    result: bool = True,
):
    """Create Banbury Anomaly Result mock record"""
    if cycle_start is None:
        cycle_start = datetime(2024, 1, 15, 10, 30, 0)
    if cycle_end is None:
        cycle_end = datetime(2024, 1, 15, 10, 32, 0)
    
    mock_record = MagicMock()
    mock_record.no = no
    mock_record.shift = shift
    mock_record.cycle_start = cycle_start
    mock_record.cycle_end = cycle_end
    mock_record.mode = mode
    mock_record.mix_duration_sec = mix_duration_sec
    mock_record.max_temp = max_temp
    mock_record.is_3_stage = is_3_stage
    mock_record.is_anomaly = is_anomaly
    mock_record.anomaly_prob = anomaly_prob
    mock_record.filtered_num = filtered_num
    mock_record.peak_count = peak_count
    mock_record.result = result
    return mock_record


@pytest.fixture
def mock_banbury_anomaly_result_rows():
    """Banbury Anomaly Result mock data"""
    return [
        create_mock_banbury_record(
            no="TEST001",
            shift=1,
            cycle_start=datetime(2024, 1, 15, 10, 30, 0),
            cycle_end=datetime(2024, 1, 15, 10, 32, 0),
            is_anomaly=False,
            result=True,
        ),
        create_mock_banbury_record(
            no="TEST002",
            shift=2,
            cycle_start=datetime(2024, 1, 15, 14, 30, 0),
            cycle_end=datetime(2024, 1, 15, 14, 32, 0),
            is_anomaly=True,
            result=False,
        ),
    ]


# ==================== Service function test ====================

@patch("app.services.banbury_anomaly_detection.get_session")
@pytest.mark.asyncio
async def test_fetch_banbury_anomaly_results(mock_get_session, mock_banbury_anomaly_result_rows):
    """fetch_banbury_anomaly_results service function test"""
    from app.services.banbury_anomaly_detection import fetch_banbury_anomaly_results

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = mock_banbury_anomaly_result_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_banbury_anomaly_results()

    # Validate
    assert len(result) == 2
    assert isinstance(result[0], BanburyAnomalyResultRecord)
    assert result[0].no == "TEST001"
    assert result[0].shift == 1
    assert result[0].is_anomaly is False
    assert result[0].result is True


@patch("app.services.banbury_anomaly_detection.get_session")
@pytest.mark.asyncio
async def test_fetch_banbury_anomaly_results_with_filters(mock_get_session, mock_banbury_anomaly_result_rows):
    """fetch_banbury_anomaly_results - filtering test"""
    from app.services.banbury_anomaly_detection import fetch_banbury_anomaly_results

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [mock_banbury_anomaly_result_rows[1]]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function (is_anomaly=True filter)
    result = await fetch_banbury_anomaly_results(is_anomaly=True)

    # Validate
    assert len(result) == 1
    assert result[0].is_anomaly is True
    assert result[0].no == "TEST002"


@patch("app.services.banbury_anomaly_detection.get_session")
@pytest.mark.asyncio
async def test_fetch_banbury_anomaly_results_with_result_filter(mock_get_session, mock_banbury_anomaly_result_rows):
    """fetch_banbury_anomaly_results - result filtering test"""
    from app.services.banbury_anomaly_detection import fetch_banbury_anomaly_results

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [mock_banbury_anomaly_result_rows[0]]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function (result=True filter)
    result = await fetch_banbury_anomaly_results(result=True)

    # Validate
    assert len(result) == 1
    assert result[0].result is True
    assert result[0].no == "TEST001"


@patch("app.services.banbury_anomaly_detection.get_session")
@pytest.mark.asyncio
async def test_fetch_banbury_anomaly_results_with_date_range(mock_get_session, mock_banbury_anomaly_result_rows):
    """fetch_banbury_anomaly_results - date range filtering test"""
    from app.services.banbury_anomaly_detection import fetch_banbury_anomaly_results

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = mock_banbury_anomaly_result_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function (date range filter)
    result = await fetch_banbury_anomaly_results(
        cycle_start_from=datetime(2024, 1, 15, 0, 0, 0),
        cycle_start_to=datetime(2024, 1, 15, 23, 59, 59),
    )

    # Validate
    assert len(result) == 2


# ==================== Date parsing function test ====================

def test_parse_cycle_start_date_yyyy():
    """_parse_cycle_start_date - yyyy format test"""
    from app.services.banbury_anomaly_detection import _parse_cycle_start_date
    kst = ZoneInfo("Asia/Seoul")

    result = _parse_cycle_start_date("2024")
    assert result == datetime(2024, 1, 1, tzinfo=kst)


def test_parse_cycle_start_date_yyyyMM():
    """_parse_cycle_start_date - yyyyMM format test"""
    from app.services.banbury_anomaly_detection import _parse_cycle_start_date
    kst = ZoneInfo("Asia/Seoul")

    result = _parse_cycle_start_date("202412")
    assert result == datetime(2024, 12, 1, tzinfo=kst)


def test_parse_cycle_start_date_yyyyMMdd():
    """_parse_cycle_start_date - yyyyMMdd format test"""
    from app.services.banbury_anomaly_detection import _parse_cycle_start_date
    kst = ZoneInfo("Asia/Seoul")

    result = _parse_cycle_start_date("20241205")
    assert result == datetime(2024, 12, 5, tzinfo=kst)


def test_parse_cycle_start_date_yyyy_MM():
    """_parse_cycle_start_date - yyyy-MM format test"""
    from app.services.banbury_anomaly_detection import _parse_cycle_start_date
    kst = ZoneInfo("Asia/Seoul")

    result = _parse_cycle_start_date("2024-12")
    assert result == datetime(2024, 12, 1, tzinfo=kst)


def test_parse_cycle_start_date_YYYY_MM_DD():
    """_parse_cycle_start_date - YYYY-MM-DD format test"""
    from app.services.banbury_anomaly_detection import _parse_cycle_start_date
    kst = ZoneInfo("Asia/Seoul")

    result = _parse_cycle_start_date("2024-12-05")
    assert result == datetime(2024, 12, 5, tzinfo=kst)


def test_parse_cycle_start_date_YYYY_MM_DD_HH_MM_SS():
    """_parse_cycle_start_date - YYYY-MM-DD HH:MM:SS format test"""
    from app.services.banbury_anomaly_detection import _parse_cycle_start_date
    kst = ZoneInfo("Asia/Seoul")

    result = _parse_cycle_start_date("2024-12-05 14:30:00")
    assert result == datetime(2024, 12, 5, 14, 30, 0, tzinfo=kst)


def test_parse_cycle_start_date_YYYY_MM_DD_HH_MM():
    """_parse_cycle_start_date - YYYY-MM-DD HH:MM format test"""
    from app.services.banbury_anomaly_detection import _parse_cycle_start_date
    kst = ZoneInfo("Asia/Seoul")

    result = _parse_cycle_start_date("2024-12-05 14:30")
    assert result == datetime(2024, 12, 5, 14, 30, 0, tzinfo=kst)


def test_parse_cycle_start_date_invalid():
    """_parse_cycle_start_date - invalid format test"""
    from app.services.banbury_anomaly_detection import _parse_cycle_start_date

    with pytest.raises(ValueError):
        _parse_cycle_start_date("invalid")


def test_resolve_cycle_start_range():
    """_resolve_cycle_start_range test"""
    from app.services.banbury_anomaly_detection import _resolve_cycle_start_range
    kst = ZoneInfo("Asia/Seoul")

    # yyyy format
    start_dt, end_dt = _resolve_cycle_start_range("2024", "2024")
    assert start_dt == datetime(2024, 1, 1, tzinfo=kst)
    assert end_dt == datetime(2025, 1, 1, tzinfo=kst)

    # yyyyMM format
    start_dt, end_dt = _resolve_cycle_start_range("202412", "202412")
    assert start_dt == datetime(2024, 12, 1, tzinfo=kst)
    assert end_dt == datetime(2025, 1, 1, tzinfo=kst)

    # yyyyMMdd format
    start_dt, end_dt = _resolve_cycle_start_range("20241205", "20241205")
    assert start_dt == datetime(2024, 12, 5, tzinfo=kst)
    assert end_dt == datetime(2024, 12, 6, tzinfo=kst)

    # YYYY-MM-DD format
    start_dt, end_dt = _resolve_cycle_start_range("2024-12-05", "2024-12-05")
    assert start_dt == datetime(2024, 12, 5, tzinfo=kst)
    assert end_dt == datetime(2024, 12, 6, tzinfo=kst)


# ==================== Schema validation test ====================

def test_banbury_anomaly_result_record_schema():
    """BanburyAnomalyResultRecord schema validation"""
    record = BanburyAnomalyResultRecord(
        no="TEST001",
        shift=1,
        cycle_start=datetime(2024, 1, 15, 10, 30, 0),
        cycle_end=datetime(2024, 1, 15, 10, 32, 0),
        mode="normal",
        mix_duration_sec=120.5,
        max_temp=150.25,
        is_3_stage=False,
        is_anomaly=False,
        anomaly_prob=0.1234,
        filtered_num=10,
        peak_count=5,
        result=True,
    )

    assert record.no == "TEST001"
    assert record.shift == 1
    assert record.is_anomaly is False
    assert record.result is True
    # Schema receives original values, so check the original value before conversion (converted in service layer)
    assert record.anomaly_prob == 0.1234


def test_banbury_anomaly_result_response_schema():
    """BanburyAnomalyResultResponse schema validation"""
    records = [
        BanburyAnomalyResultRecord(
            no="TEST001",
            shift=1,
            cycle_start=datetime(2024, 1, 15, 10, 30, 0),
            cycle_end=datetime(2024, 1, 15, 10, 32, 0),
            mode="normal",
            mix_duration_sec=120.5,
            max_temp=150.25,
            is_3_stage=False,
            is_anomaly=False,
            anomaly_prob=0.1234,
            filtered_num=10,
            peak_count=5,
            result=True,
        ),
    ]
    response = BanburyAnomalyResultResponse(data=records, total=1)

    assert response.total == 1
    assert len(response.data) == 1
    assert response.data[0].no == "TEST001"


# ==================== API endpoint test ====================

@patch("app.api.v1.banbury_anomaly_detection.fetch_banbury_anomaly_results")
def test_get_banbury_anomaly_results_endpoint(mock_fetch):
    """GET /api/v1/banbury-anomaly-detection endpoint test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        BanburyAnomalyResultRecord(
            no="TEST001",
            shift=1,
            cycle_start=datetime(2024, 1, 15, 10, 30, 0),
            cycle_end=datetime(2024, 1, 15, 10, 32, 0),
            mode="normal",
            mix_duration_sec=120.5,
            max_temp=150.25,
            is_3_stage=False,
            is_anomaly=False,
            anomaly_prob=0.1234,
            filtered_num=10,
            peak_count=5,
            result=True,
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/banbury-anomaly-detection")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert len(data["data"]) == 1
    assert data["data"][0]["no"] == "TEST001"
    assert data["data"][0]["is_anomaly"] is False
    assert data["data"][0]["result"] is True


@patch("app.api.v1.banbury_anomaly_detection.fetch_banbury_anomaly_results")
def test_get_banbury_anomaly_results_with_filters(mock_fetch):
    """GET /api/v1/banbury-anomaly-detection - filtering test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        BanburyAnomalyResultRecord(
            no="TEST002",
            shift=2,
            cycle_start=datetime(2024, 1, 15, 14, 30, 0),
            cycle_end=datetime(2024, 1, 15, 14, 32, 0),
            mode="normal",
            mix_duration_sec=120.5,
            max_temp=150.25,
            is_3_stage=False,
            is_anomaly=True,
            anomaly_prob=0.5678,
            filtered_num=10,
            peak_count=5,
            result=False,
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/banbury-anomaly-detection?is_anomaly=true")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["data"][0]["is_anomaly"] is True


@patch("app.api.v1.banbury_anomaly_detection.fetch_banbury_anomaly_results")
def test_get_banbury_anomaly_results_with_result_filter(mock_fetch):
    """GET /api/v1/banbury-anomaly-detection - result filtering test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        BanburyAnomalyResultRecord(
            no="TEST001",
            shift=1,
            cycle_start=datetime(2024, 1, 15, 10, 30, 0),
            cycle_end=datetime(2024, 1, 15, 10, 32, 0),
            mode="normal",
            mix_duration_sec=120.5,
            max_temp=150.25,
            is_3_stage=False,
            is_anomaly=False,
            anomaly_prob=0.1234,
            filtered_num=10,
            peak_count=5,
            result=True,
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/banbury-anomaly-detection?result=true")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["data"][0]["result"] is True


@patch("app.api.v1.banbury_anomaly_detection.fetch_banbury_anomaly_results")
def test_get_banbury_anomaly_results_with_date_formats(mock_fetch):
    """GET /api/v1/banbury-anomaly-detection - various date formats test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = []

    client = TestClient(app)

    # yyyy format
    response = client.get("/api/v1/banbury-anomaly-detection?cycle_start_from=2024&cycle_start_to=2024")
    assert response.status_code == 200

    # yyyyMM format
    response = client.get("/api/v1/banbury-anomaly-detection?cycle_start_from=202412&cycle_start_to=202412")
    assert response.status_code == 200

    # yyyyMMdd format
    response = client.get("/api/v1/banbury-anomaly-detection?cycle_start_from=20241205&cycle_start_to=20241205")
    assert response.status_code == 200

    # yyyy-MM format
    response = client.get("/api/v1/banbury-anomaly-detection?cycle_start_from=2024-12&cycle_start_to=2024-12")
    assert response.status_code == 200

    # YYYY-MM-DD format
    response = client.get("/api/v1/banbury-anomaly-detection?cycle_start_from=2024-12-05&cycle_start_to=2024-12-05")
    assert response.status_code == 200

    # YYYY-MM-DD HH:MM:SS format (space is encoded as %20)
    response = client.get("/api/v1/banbury-anomaly-detection?cycle_start_from=2024-12-05%2014:30:00&cycle_start_to=2024-12-05%2014:30:00")
    assert response.status_code == 200

    # YYYY-MM-DD HH:MM format (space is encoded as %20)
    response = client.get("/api/v1/banbury-anomaly-detection?cycle_start_from=2024-12-05%2014:30&cycle_start_to=2024-12-05%2014:30")
    assert response.status_code == 200


@patch("app.api.v1.banbury_anomaly_detection.fetch_banbury_anomaly_results")
def test_get_banbury_anomaly_results_invalid_date_format(mock_fetch):
    """GET /api/v1/banbury-anomaly-detection - invalid date format test"""
    from app.main import app

    client = TestClient(app)
    response = client.get("/api/v1/banbury-anomaly-detection?cycle_start_from=invalid")

    # Validate
    assert response.status_code == 400
    assert "Invalid date format" in response.json()["detail"]

