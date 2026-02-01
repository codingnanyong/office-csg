"""Tests for IPI Quality Temperature endpoints and services."""

import os
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime
from decimal import Decimal

# Test environment variable setting
os.environ.setdefault("MONTRG_DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test_db")

# Oracle initialization error prevention
if "CMMS_DATABASE_URL" in os.environ:
    del os.environ["CMMS_DATABASE_URL"]

from app.schemas.ipi_quality_temperature import (
    IpiQualityTemperatureItem,
    IpiQualityTemperatureResponse,
    IpiQualityTemperatureDetailItem,
)

# Mock data helper functions
def create_mock_main_record(
    osnd_id: int = 1001,
    osnd_dt: datetime = None,
    machine_cd: str = "MCA34",
    station: int = 1,
    station_rl: str = "L",
    mold_id: str = "MOLD001",
    reason_cd: str = "Burning",
    size_cd: str = "SIZE01",
    lr_cd: str = "L",
    osnd_bt_qty: int = 10,
):
    """Create IPI Quality Temperature main record mock"""
    if osnd_dt is None:
        osnd_dt = datetime(2024, 1, 15, 10, 30, 0)
    
    mock_record = MagicMock()
    mock_record.osnd_id = osnd_id
    mock_record.osnd_dt = osnd_dt
    mock_record.machine_cd = machine_cd
    mock_record.station = station
    mock_record.station_rl = station_rl
    mock_record.mold_id = mold_id
    mock_record.reason_cd = reason_cd
    mock_record.size_cd = size_cd
    mock_record.lr_cd = lr_cd
    mock_record.osnd_bt_qty = osnd_bt_qty
    return mock_record


def create_mock_detail_record(
    osnd_id: int = 1001,
    osnd_dt: datetime = None,
    temp_type: str = "L",
    measurement_time: datetime = None,
    temperature: Decimal = Decimal("150.25"),
    seq_no: int = 1,
    etl_extract_time: datetime = None,
    etl_ingest_time: datetime = None,
):
    """Create IPI Quality Temperature detail record mock"""
    if osnd_dt is None:
        osnd_dt = datetime(2024, 1, 15, 10, 30, 0)
    if measurement_time is None:
        measurement_time = datetime(2024, 1, 15, 10, 23, 0)
    if etl_extract_time is None:
        etl_extract_time = datetime(2024, 1, 15, 10, 30, 0)
    if etl_ingest_time is None:
        etl_ingest_time = datetime(2024, 1, 15, 10, 30, 0)
    
    mock_record = MagicMock()
    mock_record.osnd_id = osnd_id
    mock_record.osnd_dt = osnd_dt
    mock_record.temp_type = temp_type
    mock_record.measurement_time = measurement_time
    mock_record.temperature = temperature
    mock_record.seq_no = seq_no
    mock_record.etl_extract_time = etl_extract_time
    mock_record.etl_ingest_time = etl_ingest_time
    return mock_record


@pytest.fixture
def mock_main_records():
    """IPI Quality Temperature main records mock data"""
    return [
        create_mock_main_record(
            osnd_id=1001,
            osnd_dt=datetime(2024, 1, 15, 10, 30, 0),
            machine_cd="MCA34",
            station=1,
            reason_cd="Burning",
        ),
        create_mock_main_record(
            osnd_id=1002,
            osnd_dt=datetime(2024, 1, 15, 11, 30, 0),
            machine_cd="MCA34",
            station=2,
            reason_cd="Contamination",
        ),
    ]


@pytest.fixture
def mock_detail_records():
    """IPI Quality Temperature detail records mock data"""
    base_dt = datetime(2024, 1, 15, 10, 30, 0)
    return [
        # Records for osnd_id=1001
        create_mock_detail_record(
            osnd_id=1001,
            osnd_dt=base_dt,
            temp_type="L",
            measurement_time=datetime(2024, 1, 15, 10, 23, 0),
            temperature=Decimal("150.25"),
            seq_no=1,
        ),
        create_mock_detail_record(
            osnd_id=1001,
            osnd_dt=base_dt,
            temp_type="L",
            measurement_time=datetime(2024, 1, 15, 10, 24, 0),
            temperature=Decimal("151.50"),
            seq_no=2,
        ),
        create_mock_detail_record(
            osnd_id=1001,
            osnd_dt=base_dt,
            temp_type="U",
            measurement_time=datetime(2024, 1, 15, 10, 23, 0),
            temperature=Decimal("160.75"),
            seq_no=1,
        ),
        create_mock_detail_record(
            osnd_id=1001,
            osnd_dt=base_dt,
            temp_type="U",
            measurement_time=datetime(2024, 1, 15, 10, 24, 0),
            temperature=Decimal("161.00"),
            seq_no=2,
        ),
        # Records for osnd_id=1002
        create_mock_detail_record(
            osnd_id=1002,
            osnd_dt=datetime(2024, 1, 15, 11, 30, 0),
            temp_type="L",
            measurement_time=datetime(2024, 1, 15, 11, 23, 0),
            temperature=Decimal("152.00"),
            seq_no=1,
        ),
        create_mock_detail_record(
            osnd_id=1002,
            osnd_dt=datetime(2024, 1, 15, 11, 30, 0),
            temp_type="U",
            measurement_time=datetime(2024, 1, 15, 11, 23, 0),
            temperature=Decimal("162.50"),
            seq_no=1,
        ),
    ]


# ==================== Service function test ====================

@patch("app.services.ipi_quality_temperature.get_session")
@pytest.mark.asyncio
async def test_fetch_ipi_quality_temperature(mock_get_session, mock_main_records, mock_detail_records):
    """fetch_ipi_quality_temperature service function test"""
    from app.services.ipi_quality_temperature import fetch_ipi_quality_temperature

    # Mock session setting
    mock_session = AsyncMock()
    
    # First call: main query result - filtered to only osnd_id=1001
    filtered_main = [r for r in mock_main_records if r.osnd_id == 1001]
    mock_main_result = MagicMock()
    mock_main_result.scalars.return_value.all.return_value = filtered_main
    
    # Second call: detail query result - filtered to only osnd_id=1001
    filtered_details = [d for d in mock_detail_records if d.osnd_id == 1001]
    mock_detail_result = MagicMock()
    mock_detail_result.scalars.return_value.all.return_value = filtered_details
    
    # Setup execute to return different results for main and detail queries
    mock_session.execute = AsyncMock(side_effect=[mock_main_result, mock_detail_result])
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function with osnd_id
    result = await fetch_ipi_quality_temperature(osnd_id=1001)

    # Validate - should return only records matching osnd_id=1001
    assert len(result) == 1
    
    # Check record
    assert result[0]["osnd_id"] == 1001
    assert result[0]["reason_cd"] == "Burning"
    assert result[0]["machine_cd"] == "MCA34"
    assert len(result[0]["details"]) == 4  # 2 L + 2 U records
    
    # Check detail records are sorted by seq_no
    details = result[0]["details"]
    assert details[0]["temp_type"] == "L"
    assert details[0]["seq_no"] == 1
    assert details[1]["temp_type"] == "L"
    assert details[1]["seq_no"] == 2
    assert details[2]["temp_type"] == "U"
    assert details[2]["seq_no"] == 1
    assert details[3]["temp_type"] == "U"
    assert details[3]["seq_no"] == 2


@patch("app.services.ipi_quality_temperature.get_session")
@pytest.mark.asyncio
async def test_fetch_ipi_quality_temperature_with_filters(mock_get_session, mock_main_records, mock_detail_records):
    """fetch_ipi_quality_temperature - filtering test"""
    from app.services.ipi_quality_temperature import fetch_ipi_quality_temperature

    # Mock session setting - filter by osnd_id
    mock_session = AsyncMock()
    
    # Filtered main records
    filtered_main = [mock_main_records[0]]
    mock_main_result = MagicMock()
    mock_main_result.scalars.return_value.all.return_value = filtered_main
    
    # Filtered detail records (only for osnd_id=1001)
    filtered_details = [d for d in mock_detail_records if d.osnd_id == 1001]
    mock_detail_result = MagicMock()
    mock_detail_result.scalars.return_value.all.return_value = filtered_details
    
    mock_session.execute = AsyncMock(side_effect=[mock_main_result, mock_detail_result])
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function with filter
    result = await fetch_ipi_quality_temperature(osnd_id=1001)

    # Validate
    assert len(result) == 1
    assert result[0]["osnd_id"] == 1001
    assert len(result[0]["details"]) == 4


@patch("app.services.ipi_quality_temperature.get_session")
@pytest.mark.asyncio
async def test_fetch_ipi_quality_temperature_empty_result(mock_get_session):
    """fetch_ipi_quality_temperature - empty result test"""
    from app.services.ipi_quality_temperature import fetch_ipi_quality_temperature

    # Mock session setting - empty result
    mock_session = AsyncMock()
    mock_main_result = MagicMock()
    mock_main_result.scalars.return_value.all.return_value = []
    
    mock_session.execute = AsyncMock(return_value=mock_main_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function with osnd_id parameter
    result = await fetch_ipi_quality_temperature(osnd_id=9999)

    # Validate
    assert len(result) == 0
# ==================== Schema validation test ====================

def test_ipi_quality_temperature_detail_item_schema():
    """IpiQualityTemperatureDetailItem schema validation"""
    detail = IpiQualityTemperatureDetailItem(
        temp_type="L",
        measurement_time=datetime(2024, 1, 15, 10, 23, 0),
        temperature=Decimal("150.25"),
        seq_no=1,
    )

    assert detail.temp_type == "L"
    assert detail.temperature == Decimal("150.25")
    assert detail.seq_no == 1


def test_ipi_quality_temperature_item_schema():
    """IpiQualityTemperatureItem schema validation"""
    details = [
        IpiQualityTemperatureDetailItem(
            temp_type="L",
            measurement_time=datetime(2024, 1, 15, 10, 23, 0),
            temperature=Decimal("150.25"),
            seq_no=1,
        ),
    ]
    
    item = IpiQualityTemperatureItem(
        osnd_id=1001,
        osnd_dt=datetime(2024, 1, 15, 10, 30, 0),
        machine_cd="MCA34",
        station=1,
        station_rl="L",
        mold_id="MOLD001",
        reason_cd="Burning",
        size_cd="SIZE01",
        lr_cd="L",
        osnd_bt_qty=10,
        details=details,
    )

    assert item.osnd_id == 1001
    assert item.machine_cd == "MCA34"
    assert item.reason_cd == "Burning"
    assert len(item.details) == 1
    assert item.details[0].temp_type == "L"


def test_ipi_quality_temperature_response_schema():
    """IpiQualityTemperatureResponse schema validation"""
    items = [
        IpiQualityTemperatureItem(
            osnd_id=1001,
            osnd_dt=datetime(2024, 1, 15, 10, 30, 0),
            machine_cd="MCA34",
            reason_cd="Burning",
            details=[],
        ),
    ]
    response = IpiQualityTemperatureResponse(items=items, count=1)

    assert response.count == 1
    assert len(response.items) == 1
    assert response.items[0].osnd_id == 1001


# ==================== API endpoint test ====================

@patch("app.api.v1.ipi_quality_temperature.fetch_ipi_quality_temperature")
def test_get_ipi_quality_temperature_endpoint(mock_fetch):
    """GET /api/v1/ipi-quality-temperature endpoint test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        {
            "osnd_id": 1001,
            "osnd_dt": datetime(2024, 1, 15, 10, 30, 0),
            "machine_cd": "MCA34",
            "station": 1,
            "station_rl": "L",
            "mold_id": "MOLD001",
            "reason_cd": "Burning",
            "size_cd": "SIZE01",
            "lr_cd": "L",
            "osnd_bt_qty": 10,
            "details": [
                {
                    "temp_type": "L",
                    "measurement_time": datetime(2024, 1, 15, 10, 23, 0),
                    "temperature": Decimal("150.25"),
                    "seq_no": 1,
                },
            ],
        },
    ]

    client = TestClient(app)
    response = client.get("/api/v1/ipi-quality-temperature?osnd_id=1001")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert len(data["items"]) == 1
    assert data["items"][0]["osnd_id"] == 1001
    assert data["items"][0]["reason_cd"] == "Burning"
    assert len(data["items"][0]["details"]) == 1


@patch("app.api.v1.ipi_quality_temperature.fetch_ipi_quality_temperature")
def test_get_ipi_quality_temperature_with_filters(mock_fetch):
    """GET /api/v1/ipi-quality-temperature - filtering test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        {
            "osnd_id": 1001,
            "osnd_dt": datetime(2024, 1, 15, 10, 30, 0),
            "machine_cd": "MCA34",
            "reason_cd": "Burning",
            "details": [],
        },
    ]

    client = TestClient(app)
    response = client.get("/api/v1/ipi-quality-temperature?osnd_id=1001&machine_cd=MCA34")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert data["items"][0]["osnd_id"] == 1001
    assert data["items"][0]["machine_cd"] == "MCA34"


@patch("app.api.v1.ipi_quality_temperature.fetch_ipi_quality_temperature")
def test_get_ipi_quality_temperature_not_found(mock_fetch):
    """GET /api/v1/ipi-quality-temperature - not found test"""
    from app.main import app

    # Mock setting - empty result
    mock_fetch.return_value = []

    client = TestClient(app)
    response = client.get("/api/v1/ipi-quality-temperature?osnd_id=9999")

    # Validate
    assert response.status_code == 404
    assert "No data found" in response.json()["detail"]

