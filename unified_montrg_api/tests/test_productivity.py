"""Tests for Productivity endpoints and services."""

import os
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import date

# Test environment variable setting
os.environ.setdefault("MONTRG_DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test_db")

# Prevent Oracle initialization error
# productivity test only uses PostgreSQL, so prevent Oracle initialization
if "CMMS_DATABASE_URL" in os.environ:
    del os.environ["CMMS_DATABASE_URL"]

from app.schemas.productivity_op_cd import ProductivityOpCdRecord, ProductivityOpCdResponse
from app.schemas.productivity_op_group import ProductivityOpGroupRecord, ProductivityOpGroupResponse
from app.schemas.productivity_ip_zone import ProductivityIpZoneRecord, ProductivityIpZoneResponse
from app.schemas.productivity_ip_machine import ProductivityIpMachineRecord, ProductivityIpMachineResponse
from app.schemas.productivity_ph_zone import ProductivityPhZoneRecord, ProductivityPhZoneResponse
from app.schemas.productivity_ph_machine import ProductivityPhMachineRecord, ProductivityPhMachineResponse


# Mock data helper function
def create_mock_row(*values):
    """Create mock row object (tuple form)"""
    return tuple(values)


@pytest.fixture
def mock_productivity_op_cd_rows():
    """Productivity OP CD mock data"""
    return [
        create_mock_row(
            date(2024, 1, 15),
            "OP001",
            "GROUP01",
            1000.0,
            950.0,
            50.0,
            0.95,
        ),
        create_mock_row(
            date(2024, 1, 14),
            "OP001",
            "GROUP01",
            1000.0,
            980.0,
            20.0,
            0.98,
        ),
    ]


@pytest.fixture
def mock_productivity_op_group_rows():
    """Productivity OP Group mock data"""
    return [
        create_mock_row(
            date(2024, 1, 15),
            "GROUP01",
            5000.0,
            4800.0,
            200.0,
            0.96,
        ),
        create_mock_row(
            date(2024, 1, 14),
            "GROUP01",
            5000.0,
            4900.0,
            100.0,
            0.98,
        ),
    ]


@pytest.fixture
def mock_productivity_ip_zone_rows():
    """Productivity IP Zone mock data"""
    return [
        create_mock_row(
            date(2024, 1, 15),
            "ZONE001",
            3000.0,
            2900.0,
            100.0,
            0.97,
        ),
        create_mock_row(
            date(2024, 1, 14),
            "ZONE001",
            3000.0,
            2950.0,
            50.0,
            0.98,
        ),
    ]


@pytest.fixture
def mock_productivity_ph_zone_rows():
    """Productivity PH Zone mock data"""
    return [
        create_mock_row(
            date(2024, 1, 15),
            "LINE001",
            2000.0,
            1900.0,
            100.0,
            0.95,
        ),
        create_mock_row(
            date(2024, 1, 14),
            "LINE001",
            2000.0,
            1950.0,
            50.0,
            0.98,
        ),
    ]


@pytest.fixture
def mock_productivity_ip_machine_rows():
    """Productivity IP Machine mock data"""
    return [
        create_mock_row(
            date(2024, 1, 15),
            "MCA02",
            1500.0,
            1450.0,
            50.0,
            0.97,
        ),
        create_mock_row(
            date(2024, 1, 14),
            "MCA02",
            1500.0,
            1480.0,
            20.0,
            0.99,
        ),
    ]


@pytest.fixture
def mock_productivity_ph_machine_rows():
    """Productivity PH Machine mock data"""
    return [
        create_mock_row(
            date(2024, 1, 15),
            "PH01",
            1200.0,
            1150.0,
            50.0,
            0.96,
        ),
        create_mock_row(
            date(2024, 1, 14),
            "PH01",
            1200.0,
            1180.0,
            20.0,
            0.98,
        ),
    ]


# ==================== Service function test ====================

@patch("app.services.productivity_op_cd.get_session")
@pytest.mark.asyncio
async def test_fetch_productivity_op_cd(mock_get_session, mock_productivity_op_cd_rows):
    """fetch_productivity_op_cd service function test"""
    from app.services.productivity_op_cd import fetch_productivity_op_cd

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_productivity_op_cd_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_productivity_op_cd(op_cd="OP001")

    # Validate
    assert len(result) == 2
    assert isinstance(result[0], ProductivityOpCdRecord)
    assert result[0].op_cd == "OP001"
    assert result[0].op_group == "GROUP01"
    assert result[0].plan_qty == 1000.0
    assert result[0].prod_qty == 950.0
    assert result[0].defect_qty == 50.0
    assert result[0].quality_rate == 0.95


@patch("app.services.productivity_op_cd.get_session")
@pytest.mark.asyncio
async def test_fetch_productivity_op_cd_with_date(mock_get_session, mock_productivity_op_cd_rows):
    """fetch_productivity_op_cd - date filtering test"""
    from app.services.productivity_op_cd import fetch_productivity_op_cd

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = [mock_productivity_op_cd_rows[0]]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_productivity_op_cd(op_cd="OP001", date=date(2024, 1, 15))

    # Validate
    assert len(result) == 1
    assert result[0].business_date == date(2024, 1, 15)


@patch("app.services.productivity_op_group.get_session")
@pytest.mark.asyncio
async def test_fetch_productivity_op_group(mock_get_session, mock_productivity_op_group_rows):
    """fetch_productivity_op_group service function test"""
    from app.services.productivity_op_group import fetch_productivity_op_group

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_productivity_op_group_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_productivity_op_group(op_group="GROUP01")

    # Validate
    assert len(result) == 2
    assert isinstance(result[0], ProductivityOpGroupRecord)
    assert result[0].op_group == "GROUP01"
    assert result[0].plan_qty == 5000.0
    assert result[0].prod_qty == 4800.0


@patch("app.services.productivity_ip_zone.get_session")
@pytest.mark.asyncio
async def test_fetch_productivity_ip_zone(mock_get_session, mock_productivity_ip_zone_rows):
    """fetch_productivity_ip_zone service function test"""
    from app.services.productivity_ip_zone import fetch_productivity_ip_zone

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_productivity_ip_zone_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_productivity_ip_zone(zone_cd="ZONE001")

    # Validate
    assert len(result) == 2
    assert isinstance(result[0], ProductivityIpZoneRecord)
    assert result[0].zone_cd == "ZONE001"
    assert result[0].plan_qty == 3000.0
    assert result[0].prod_qty == 2900.0


@patch("app.services.productivity_ph_zone.get_session")
@pytest.mark.asyncio
async def test_fetch_productivity_ph_zone(mock_get_session, mock_productivity_ph_zone_rows):
    """fetch_productivity_ph_zone service function test"""
    from app.services.productivity_ph_zone import fetch_productivity_ph_zone

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_productivity_ph_zone_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_productivity_ph_zone(line_group="LINE001")

    # Validate
    assert len(result) == 2
    assert isinstance(result[0], ProductivityPhZoneRecord)
    assert result[0].line_group == "LINE001"
    assert result[0].plan_qty == 2000.0
    assert result[0].prod_qty == 1900.0


@patch("app.services.productivity_ip_machine.get_session")
@pytest.mark.asyncio
async def test_fetch_productivity_ip_machine(mock_get_session, mock_productivity_ip_machine_rows):
    """fetch_productivity_ip_machine service function test"""
    from app.services.productivity_ip_machine import fetch_productivity_ip_machine

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_productivity_ip_machine_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_productivity_ip_machine(machine_cd="MCA02")

    # Validate
    assert len(result) == 2
    assert isinstance(result[0], ProductivityIpMachineRecord)
    assert result[0].machine_cd == "MCA02"
    assert result[0].plan_qty == 1500.0
    assert result[0].prod_qty == 1450.0
    assert result[0].defect_qty == 50.0
    assert result[0].quality_rate == 0.97


@patch("app.services.productivity_ip_machine.get_session")
@pytest.mark.asyncio
async def test_fetch_productivity_ip_machine_with_date(mock_get_session, mock_productivity_ip_machine_rows):
    """fetch_productivity_ip_machine - date filtering test"""
    from app.services.productivity_ip_machine import fetch_productivity_ip_machine

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = [mock_productivity_ip_machine_rows[0]]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_productivity_ip_machine(machine_cd="MCA02", date=date(2024, 1, 15))

    # Validate
    assert len(result) == 1
    assert result[0].business_date == date(2024, 1, 15)


@patch("app.services.productivity_ph_machine.get_session")
@pytest.mark.asyncio
async def test_fetch_productivity_ph_machine(mock_get_session, mock_productivity_ph_machine_rows):
    """fetch_productivity_ph_machine service function test"""
    from app.services.productivity_ph_machine import fetch_productivity_ph_machine

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_productivity_ph_machine_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_productivity_ph_machine(machine_cd="PH01")

    # Validate
    assert len(result) == 2
    assert isinstance(result[0], ProductivityPhMachineRecord)
    assert result[0].machine_cd == "PH01"
    assert result[0].plan_qty == 1200.0
    assert result[0].prod_qty == 1150.0
    assert result[0].defect_qty == 50.0
    assert result[0].quality_rate == 0.96


@patch("app.services.productivity_ph_machine.get_session")
@pytest.mark.asyncio
async def test_fetch_productivity_ph_machine_with_date(mock_get_session, mock_productivity_ph_machine_rows):
    """fetch_productivity_ph_machine - date filtering test"""
    from app.services.productivity_ph_machine import fetch_productivity_ph_machine

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = [mock_productivity_ph_machine_rows[0]]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_productivity_ph_machine(machine_cd="PH01", date=date(2024, 1, 15))

    # Validate
    assert len(result) == 1
    assert result[0].business_date == date(2024, 1, 15)


# ==================== Schema validation test ====================

def test_productivity_op_cd_record_schema():
    """ProductivityOpCdRecord schema validation"""
    record = ProductivityOpCdRecord(
        business_date=date(2024, 1, 15),
        op_cd="OP001",
        op_group="GROUP01",
        plan_qty=1000.0,
        prod_qty=950.0,
        defect_qty=50.0,
        quality_rate=0.95,
    )

    assert record.business_date == date(2024, 1, 15)
    assert record.op_cd == "OP001"
    assert record.op_group == "GROUP01"
    assert record.plan_qty == 1000.0
    assert record.quality_rate == 0.95


def test_productivity_op_cd_response_schema():
    """ProductivityOpCdResponse schema validation"""
    records = [
        ProductivityOpCdRecord(
            business_date=date(2024, 1, 15),
            op_cd="OP001",
            op_group="GROUP01",
            plan_qty=1000.0,
        ),
    ]
    response = ProductivityOpCdResponse(data=records, total=1)

    assert response.total == 1
    assert len(response.data) == 1
    assert response.data[0].op_cd == "OP001"


def test_productivity_op_group_record_schema():
    """ProductivityOpGroupRecord schema validation"""
    record = ProductivityOpGroupRecord(
        business_date=date(2024, 1, 15),
        op_group="GROUP01",
        plan_qty=5000.0,
        prod_qty=4800.0,
        defect_qty=200.0,
        quality_rate=0.96,
    )

    assert record.business_date == date(2024, 1, 15)
    assert record.op_group == "GROUP01"
    assert record.plan_qty == 5000.0


def test_productivity_ip_zone_record_schema():
    """ProductivityIpZoneRecord schema validation"""
    record = ProductivityIpZoneRecord(
        business_date=date(2024, 1, 15),
        zone_cd="ZONE001",
        plan_qty=3000.0,
        prod_qty=2900.0,
        defect_qty=100.0,
        quality_rate=0.97,
    )

    assert record.business_date == date(2024, 1, 15)
    assert record.zone_cd == "ZONE001"
    assert record.prod_qty == 2900.0


def test_productivity_ph_zone_record_schema():
    """ProductivityPhZoneRecord schema validation"""
    record = ProductivityPhZoneRecord(
        business_date=date(2024, 1, 15),
        line_group="LINE001",
        plan_qty=2000.0,
        prod_qty=1900.0,
        defect_qty=100.0,
        quality_rate=0.95,
    )

    assert record.business_date == date(2024, 1, 15)
    assert record.line_group == "LINE001"
    assert record.prod_qty == 1900.0


def test_productivity_ip_machine_record_schema():
    """ProductivityIpMachineRecord schema validation"""
    record = ProductivityIpMachineRecord(
        business_date=date(2024, 1, 15),
        machine_cd="MCA02",
        plan_qty=1500.0,
        prod_qty=1450.0,
        defect_qty=50.0,
        quality_rate=0.97,
    )

    assert record.business_date == date(2024, 1, 15)
    assert record.machine_cd == "MCA02"
    assert record.prod_qty == 1450.0
    assert record.defect_qty == 50.0
    assert record.quality_rate == 0.97


def test_productivity_ip_machine_response_schema():
    """ProductivityIpMachineResponse schema validation"""
    records = [
        ProductivityIpMachineRecord(
            business_date=date(2024, 1, 15),
            machine_cd="MCA02",
            plan_qty=1500.0,
            prod_qty=1450.0,
            defect_qty=50.0,
            quality_rate=0.97,
        ),
    ]
    response = ProductivityIpMachineResponse(data=records, total=1)

    assert response.total == 1
    assert len(response.data) == 1
    assert response.data[0].machine_cd == "MCA02"


def test_productivity_ph_machine_record_schema():
    """ProductivityPhMachineRecord schema validation"""
    record = ProductivityPhMachineRecord(
        business_date=date(2024, 1, 15),
        machine_cd="PH01",
        plan_qty=1200.0,
        prod_qty=1150.0,
        defect_qty=50.0,
        quality_rate=0.96,
    )

    assert record.business_date == date(2024, 1, 15)
    assert record.machine_cd == "PH01"
    assert record.prod_qty == 1150.0
    assert record.defect_qty == 50.0
    assert record.quality_rate == 0.96


def test_productivity_ph_machine_response_schema():
    """ProductivityPhMachineResponse schema validation"""
    records = [
        ProductivityPhMachineRecord(
            business_date=date(2024, 1, 15),
            machine_cd="PH01",
            plan_qty=1200.0,
            prod_qty=1150.0,
            defect_qty=50.0,
            quality_rate=0.96,
        ),
    ]
    response = ProductivityPhMachineResponse(data=records, total=1)

    assert response.total == 1
    assert len(response.data) == 1
    assert response.data[0].machine_cd == "PH01"


# ==================== API endpoint test ====================

@patch("app.api.v1.productivity.fetch_productivity_op_cd", new_callable=AsyncMock)
def test_get_productivity_op_cd_endpoint(mock_fetch):
    """GET /api/v1/productivity/op-cd endpoint test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        ProductivityOpCdRecord(
            business_date=date(2024, 1, 15),
            op_cd="OP001",
            op_group="GROUP01",
            plan_qty=1000.0,
            prod_qty=950.0,
            defect_qty=50.0,
            quality_rate=0.95,
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/productivity/op-cd?op_cd=OP001")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert len(data["data"]) == 1
    assert data["data"][0]["op_cd"] == "OP001"
    assert data["data"][0]["op_group"] == "GROUP01"
    # Default (no date) should call realtime path with date=None
    assert mock_fetch.await_args.kwargs["date"] is None


@patch("app.api.v1.productivity.fetch_productivity_op_group", new_callable=AsyncMock)
def test_get_productivity_op_group_endpoint(mock_fetch):
    """GET /api/v1/productivity/op-group endpoint test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        ProductivityOpGroupRecord(
            business_date=date(2024, 1, 15),
            op_group="GROUP01",
            plan_qty=5000.0,
            prod_qty=4800.0,
            defect_qty=200.0,
            quality_rate=0.96,
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/productivity/op-group?op_group=GROUP01")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert len(data["data"]) == 1
    assert data["data"][0]["op_group"] == "GROUP01"


@patch("app.api.v1.productivity.fetch_productivity_ip_zone", new_callable=AsyncMock)
def test_get_productivity_ip_zone_endpoint(mock_fetch):
    """GET /api/v1/productivity/ip-zone endpoint test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        ProductivityIpZoneRecord(
            business_date=date(2024, 1, 15),
            zone_cd="ZONE001",
            plan_qty=3000.0,
            prod_qty=2900.0,
            defect_qty=100.0,
            quality_rate=0.97,
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/productivity/ip-zone?zone_cd=ZONE001")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert len(data["data"]) == 1
    assert data["data"][0]["zone_cd"] == "ZONE001"


@patch("app.api.v1.productivity.fetch_productivity_ph_zone", new_callable=AsyncMock)
def test_get_productivity_ph_line_endpoint(mock_fetch):
    """GET /api/v1/productivity/ph-line endpoint test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        ProductivityPhZoneRecord(
            business_date=date(2024, 1, 15),
            line_group="LINE001",
            plan_qty=2000.0,
            prod_qty=1900.0,
            defect_qty=100.0,
            quality_rate=0.95,
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/productivity/ph-line?line_group=LINE001")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert len(data["data"]) == 1
    assert data["data"][0]["line_group"] == "LINE001"


@patch("app.api.v1.productivity.fetch_productivity_op_cd", new_callable=AsyncMock)
def test_get_productivity_op_cd_with_date(mock_fetch):
    """GET /api/v1/productivity/op-cd - date parameter test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        ProductivityOpCdRecord(
            business_date=date(2024, 1, 15),
            op_cd="OP001",
            op_group="GROUP01",
            plan_qty=1000.0,
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/productivity/op-cd?op_cd=OP001&date=20240115")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    mock_fetch.assert_awaited_once()
    # Validate if date parameter is correctly parsed
    call_args = mock_fetch.call_args
    assert call_args.kwargs["date"] == date(2024, 1, 15)


@patch("app.api.v1.productivity.fetch_productivity_op_cd", new_callable=AsyncMock)
def test_get_productivity_op_cd_invalid_date(mock_fetch):
    """GET /api/v1/productivity/op-cd - invalid date format test"""
    from app.main import app

    client = TestClient(app)
    response = client.get("/api/v1/productivity/op-cd?op_cd=OP001&date=invalid")

    # Validate
    assert response.status_code == 400
    assert "Invalid date format" in response.json()["detail"]


@patch("app.api.v1.productivity.fetch_productivity_ip_machine", new_callable=AsyncMock)
def test_get_productivity_ip_machine_endpoint(mock_fetch):
    """GET /api/v1/productivity/ip-machine endpoint test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        ProductivityIpMachineRecord(
            business_date=date(2024, 1, 15),
            machine_cd="MCA02",
            plan_qty=1500.0,
            prod_qty=1450.0,
            defect_qty=50.0,
            quality_rate=0.97,
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/productivity/ip-machine?machine_cd=MCA02")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert len(data["data"]) == 1
    assert data["data"][0]["machine_cd"] == "MCA02"
    assert data["data"][0]["plan_qty"] == 1500.0
    assert data["data"][0]["prod_qty"] == 1450.0
    # Default (no date) should call realtime path with date=None
    assert mock_fetch.await_args.kwargs["date"] is None


@patch("app.api.v1.productivity.fetch_productivity_ip_machine", new_callable=AsyncMock)
def test_get_productivity_ip_machine_with_date(mock_fetch):
    """GET /api/v1/productivity/ip-machine - date parameter test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        ProductivityIpMachineRecord(
            business_date=date(2024, 1, 15),
            machine_cd="MCA02",
            plan_qty=1500.0,
            prod_qty=1450.0,
            defect_qty=50.0,
            quality_rate=0.97,
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/productivity/ip-machine?machine_cd=MCA02&date=20240115")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    mock_fetch.assert_awaited_once()
    # Validate if date parameter is correctly parsed
    call_args = mock_fetch.call_args
    assert call_args.kwargs["date"] == date(2024, 1, 15)


@patch("app.api.v1.productivity.fetch_productivity_ph_machine", new_callable=AsyncMock)
def test_get_productivity_ph_machine_endpoint(mock_fetch):
    """GET /api/v1/productivity/ph-machine endpoint test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        ProductivityPhMachineRecord(
            business_date=date(2024, 1, 15),
            machine_cd="PH01",
            plan_qty=1200.0,
            prod_qty=1150.0,
            defect_qty=50.0,
            quality_rate=0.96,
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/productivity/ph-machine?machine_cd=PH01")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert len(data["data"]) == 1
    assert data["data"][0]["machine_cd"] == "PH01"
    assert data["data"][0]["plan_qty"] == 1200.0
    assert data["data"][0]["prod_qty"] == 1150.0


@patch("app.api.v1.productivity.fetch_productivity_ph_machine", new_callable=AsyncMock)
def test_get_productivity_ph_machine_with_date(mock_fetch):
    """GET /api/v1/productivity/ph-machine - date parameter test"""
    from app.main import app

    # Mock setting
    mock_fetch.return_value = [
        ProductivityPhMachineRecord(
            business_date=date(2024, 1, 15),
            machine_cd="PH01",
            plan_qty=1200.0,
            prod_qty=1150.0,
            defect_qty=50.0,
            quality_rate=0.96,
        ),
    ]

    client = TestClient(app)
    response = client.get("/api/v1/productivity/ph-machine?machine_cd=PH01&date=2024-01-15")

    # Validate
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    mock_fetch.assert_awaited_once()
    # Validate if date parameter is correctly parsed
    call_args = mock_fetch.call_args
    assert call_args.kwargs["date"] == date(2024, 1, 15)
