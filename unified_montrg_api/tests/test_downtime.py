"""Tests for Downtime service."""

import os
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import date

# Test environment variable setting
# Prevent Oracle initialization error
if "CMMS_DATABASE_URL" in os.environ:
    del os.environ["CMMS_DATABASE_URL"]

from app.schemas.downtime import DowntimeRecord, DowntimeResponse
from app.schemas.downtime_line import DowntimeLineRecord, DowntimeLineResponse
from app.schemas.downtime_machine import DowntimeMachineRecord, DowntimeMachineResponse


# Mock data helper function
def create_mock_row(*values):
    """Create mock row object (tuple form)"""
    return tuple(values)


@pytest.fixture
def mock_downtime_rows():
    """Downtime mock data"""
    return [
        create_mock_row(
            "FACTORY01",
            "Factory 01",
            "BUILDING01",
            "Building 01",
            "LINE01",
            "Outsole",
            "OS",
            date(2024, 1, 1),
            100.0,
            95.0,
        ),
        create_mock_row(
            "FACTORY01",
            "Factory 01",
            "BUILDING01",
            "Building 01",
            "LINE02",
            "IP",
            "IP",
            date(2024, 1, 1),
            120.0,
            110.0,
        ),
    ]


@pytest.fixture
def mock_downtime_line_rows():
    """Downtime Line mock data (includes JOIN result)"""
    return [
        create_mock_row(
            date(2024, 1, 15),
            "F001",
            "Factory 01",  # factory_nm
            "B001",
            "Building 01",  # building_nm
            "LINE001",
            "Line 01",  # line_nm
            100.0,
            95.0,
        ),
        create_mock_row(
            date(2024, 1, 14),
            "F001",
            "Factory 01",  # factory_nm
            "B001",
            "Building 01",  # building_nm
            "LINE001",
            "Line 01",  # line_nm
            100.0,
            98.0,
        ),
    ]


@pytest.fixture
def mock_downtime_machine_rows():
    """Downtime Machine mock data (includes JOIN result)"""
    return [
        create_mock_row(
            date(2024, 1, 15),
            "F001",
            "Factory 01",  # factory_nm
            "B001",
            "Building 01",  # building_nm
            "LINE001",
            "Line 01",  # line_nm
            "MCA02",
            "Machine 02",
            100.0,
            95.0,
        ),
        create_mock_row(
            date(2024, 1, 14),
            "F001",
            "Factory 01",  # factory_nm
            "B001",
            "Building 01",  # building_nm
            "LINE001",
            "Line 01",  # line_nm
            "MCA02",
            "Machine 02",
            100.0,
            98.0,
        ),
    ]


# ==================== helper function test ====================

def test_get_first_day_of_month():
    """get_first_day_of_month helper function test"""
    from app.services.downtime import get_first_day_of_month

    # Normal case
    assert get_first_day_of_month(date(2024, 1, 15)) == date(2024, 1, 1)
    assert get_first_day_of_month(date(2024, 2, 28)) == date(2024, 2, 1)
    assert get_first_day_of_month(date(2024, 12, 31)) == date(2024, 12, 1)
    
    # Already first day of month
    assert get_first_day_of_month(date(2024, 1, 1)) == date(2024, 1, 1)


# ==================== Service function test ====================

@patch("app.services.downtime.get_sync_engine")
@pytest.mark.asyncio
async def test_fetch_downtime_default(mock_get_sync_engine, mock_downtime_rows):
    """fetch_downtime - default query test (current month)"""
    from app.services.downtime import fetch_downtime

    # Mock engine setting
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchall.return_value = mock_downtime_rows
    mock_conn.execute.return_value = mock_result
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
    mock_get_sync_engine.return_value = mock_engine

    # Call service function
    result = await fetch_downtime()

    # Validate
    assert len(result) == 2
    assert isinstance(result[0], DowntimeRecord)
    assert result[0].factory == "FACTORY01"
    assert result[0].line_nm == "Outsole"
    assert result[0].process == "OS"
    assert result[0].down_time_target == 100.0
    assert result[0].down_time_value == 95.0


@patch("app.services.downtime.get_sync_engine")
@pytest.mark.asyncio
async def test_fetch_downtime_with_date(mock_get_sync_engine, mock_downtime_rows):
    """fetch_downtime - date filter test"""
    from app.services.downtime import fetch_downtime

    # Mock engine setting
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchall.return_value = [mock_downtime_rows[0]]
    mock_conn.execute.return_value = mock_result
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
    mock_get_sync_engine.return_value = mock_engine

    # Call service function
    result = await fetch_downtime(date=date(2024, 1, 15))

    # Validate
    assert len(result) == 1
    assert result[0].date == date(2024, 1, 1)  # Converted to first day


@patch("app.services.downtime.get_sync_engine")
@pytest.mark.asyncio
async def test_fetch_downtime_with_process(mock_get_sync_engine, mock_downtime_rows):
    """fetch_downtime - process filter test"""
    from app.services.downtime import fetch_downtime

    # Mock engine setting
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    # Return only OS process
    mock_result.fetchall.return_value = [mock_downtime_rows[0]]
    mock_conn.execute.return_value = mock_result
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
    mock_get_sync_engine.return_value = mock_engine

    # Call service function
    result = await fetch_downtime(process="OS")

    # Validate
    assert len(result) == 1
    assert result[0].process == "OS"
    assert result[0].line_nm == "Outsole"


@patch("app.services.downtime.get_sync_engine")
@pytest.mark.asyncio
async def test_fetch_downtime_with_invalid_process(mock_get_sync_engine):
    """fetch_downtime - invalid process filter test"""
    from app.services.downtime import fetch_downtime

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
    result = await fetch_downtime(process="INVALID")

    # Validate
    assert len(result) == 0


@patch("app.services.downtime.get_sync_engine")
@pytest.mark.asyncio
async def test_fetch_downtime_empty_result(mock_get_sync_engine):
    """fetch_downtime - empty result test"""
    from app.services.downtime import fetch_downtime

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
    result = await fetch_downtime()

    # Validate
    assert len(result) == 0


# ==================== API endpoint test ====================

@pytest.mark.asyncio
async def test_get_downtime_endpoint_default():
    """Downtime API endpoint test - default query"""
    # Prevent Oracle initialization before router import
    if "CMMS_DATABASE_URL" in os.environ:
        del os.environ["CMMS_DATABASE_URL"]
    
    from fastapi import FastAPI
    from app.api.v1.downtime import router
    
    # Create independent FastAPI app
    test_app = FastAPI()
    test_app.include_router(router, prefix="/api/v1")
    from fastapi.testclient import TestClient
    client = TestClient(test_app)
    
    # Mock setting
    mock_records = [
        DowntimeRecord(
            factory="FACTORY01",
            factory_nm="Factory 01",
            building="BUILDING01",
            building_nm="Building 01",
            line="LINE01",
            line_nm="Outsole",
            process="OS",
            date=date(2024, 1, 1),
            down_time_target=100.0,
            down_time_value=95.0,
        ),
    ]
    
    with patch("app.api.v1.downtime.fetch_downtime", new_callable=AsyncMock, return_value=mock_records):
        # API call
        response = client.get("/api/v1/downtime")

        # Validate
        if response.status_code != 200:
            print(f"Response status: {response.status_code}")
            print(f"Response body: {response.text}")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        assert "data" in data
        assert "total" in data
        assert data["total"] == 1
        assert len(data["data"]) == 1
        assert data["data"][0]["process"] == "OS"


@pytest.mark.asyncio
async def test_get_downtime_endpoint_with_date():
    """Downtime API endpoint test - date filter"""
    # Prevent Oracle initialization before router import
    if "CMMS_DATABASE_URL" in os.environ:
        del os.environ["CMMS_DATABASE_URL"]
    
    from fastapi import FastAPI
    from app.api.v1.downtime import router
    
    # Create independent FastAPI app
    test_app = FastAPI()
    test_app.include_router(router, prefix="/api/v1")
    from fastapi.testclient import TestClient
    client = TestClient(test_app)
    
    # Mock setting
    mock_records = [
        DowntimeRecord(
            factory="FACTORY01",
            factory_nm="Factory 01",
            building="BUILDING01",
            building_nm="Building 01",
            line="LINE01",
            line_nm="Outsole",
            process="OS",
            date=date(2024, 1, 1),
            down_time_target=100.0,
            down_time_value=95.0,
        ),
    ]
    
    with patch("app.api.v1.downtime.fetch_downtime", new_callable=AsyncMock, return_value=mock_records):
        # API call - yyyy-MM-dd format
        response = client.get("/api/v1/downtime?date=2024-01-15")

        # Validate
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1


@pytest.mark.asyncio
async def test_get_downtime_endpoint_with_process():
    """Downtime API endpoint test - process filter"""
    # Prevent Oracle initialization before router import
    if "CMMS_DATABASE_URL" in os.environ:
        del os.environ["CMMS_DATABASE_URL"]
    
    from fastapi import FastAPI
    from app.api.v1.downtime import router
    
    # Create independent FastAPI app
    test_app = FastAPI()
    test_app.include_router(router, prefix="/api/v1")
    from fastapi.testclient import TestClient
    client = TestClient(test_app)
    
    # Mock setting
    mock_records = [
        DowntimeRecord(
            factory="FACTORY01",
            factory_nm="Factory 01",
            building="BUILDING01",
            building_nm="Building 01",
            line="LINE01",
            line_nm="Outsole",
            process="OS",
            date=date(2024, 1, 1),
            down_time_target=100.0,
            down_time_value=95.0,
        ),
    ]
    
    with patch("app.api.v1.downtime.fetch_downtime", new_callable=AsyncMock, return_value=mock_records):
        # API call
        response = client.get("/api/v1/downtime?process=os")

        # Validate
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["data"][0]["process"] == "OS"


@pytest.mark.asyncio
async def test_get_downtime_endpoint_invalid_process():
    """Downtime API endpoint test - invalid process code"""
    # Prevent Oracle initialization before router import
    if "CMMS_DATABASE_URL" in os.environ:
        del os.environ["CMMS_DATABASE_URL"]
    
    from fastapi import FastAPI
    from app.api.v1.downtime import router
    
    # Create independent FastAPI app
    test_app = FastAPI()
    test_app.include_router(router, prefix="/api/v1")
    from fastapi.testclient import TestClient
    client = TestClient(test_app)
    
    # API call - invalid process code
    response = client.get("/api/v1/downtime?process=invalid")

    # Validate
    assert response.status_code == 400
    data = response.json()
    assert "detail" in data
    assert "process must be one of" in data["detail"]


@pytest.mark.asyncio
async def test_get_downtime_endpoint_invalid_date_format():
    """Downtime API endpoint test - invalid date format"""
    # Prevent Oracle initialization before router import
    if "CMMS_DATABASE_URL" in os.environ:
        del os.environ["CMMS_DATABASE_URL"]
    
    from fastapi import FastAPI
    from app.api.v1.downtime import router
    
    # Create independent FastAPI app
    test_app = FastAPI()
    test_app.include_router(router, prefix="/api/v1")
    from fastapi.testclient import TestClient
    client = TestClient(test_app)
    
    # API call - invalid date format
    response = client.get("/api/v1/downtime?date=invalid-date")

    # Validate
    assert response.status_code == 500
    data = response.json()
    assert "detail" in data
    assert "Invalid date format" in data["detail"]


@pytest.mark.asyncio
async def test_get_downtime_endpoint_error():
    """Downtime API endpoint test - error handling"""
    # Prevent Oracle initialization before router import
    if "CMMS_DATABASE_URL" in os.environ:
        del os.environ["CMMS_DATABASE_URL"]
    
    from fastapi import FastAPI
    from app.api.v1.downtime import router
    
    # Create independent FastAPI app
    test_app = FastAPI()
    test_app.include_router(router, prefix="/api/v1")
    from fastapi.testclient import TestClient
    client = TestClient(test_app)
    
    # Mock setting - exception raised
    with patch("app.api.v1.downtime.fetch_downtime", new_callable=AsyncMock, side_effect=Exception("Database connection error")):
        # API call
        response = client.get("/api/v1/downtime")

        # Validate
        assert response.status_code == 500
        data = response.json()
        assert "detail" in data
        assert "Internal server error" in data["detail"]


# ==================== Schema validation test ====================

def test_downtime_record_schema():
    """DowntimeRecord schema validation"""
    record = DowntimeRecord(
        factory="FACTORY01",
        factory_nm="Factory 01",
        building="BUILDING01",
        building_nm="Building 01",
        line="LINE01",
        line_nm="Outsole",
        process="OS",
        date=date(2024, 1, 1),
        down_time_target=100.0,
        down_time_value=95.0,
    )

    assert record.factory == "FACTORY01"
    assert record.process == "OS"
    assert record.down_time_target == 100.0


def test_downtime_response_schema():
    """DowntimeResponse schema validation"""
    records = [
        DowntimeRecord(
            factory="FACTORY01",
            factory_nm="Factory 01",
            building="BUILDING01",
            building_nm="Building 01",
            line="LINE01",
            line_nm="Outsole",
            process="OS",
            date=date(2024, 1, 1),
            down_time_target=100.0,
            down_time_value=95.0,
        ),
    ]
    response = DowntimeResponse(data=records, total=1)

    assert response.total == 1
    assert len(response.data) == 1
    assert response.data[0].process == "OS"


# ==================== Downtime Line service function test ====================

@patch("app.services.downtime_line.get_session")
@pytest.mark.asyncio
async def test_fetch_downtime_line(mock_get_session, mock_downtime_line_rows):
    """fetch_downtime_line service function test"""
    from app.services.downtime_line import fetch_downtime_line

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_downtime_line_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_downtime_line(line_cd="LINE001")

    # Validate
    assert len(result) == 2
    assert isinstance(result[0], DowntimeLineRecord)
    assert result[0].line_cd == "LINE001"
    assert result[0].factory == "F001"
    assert result[0].factory_nm == "Factory 01"
    assert result[0].building == "B001"
    assert result[0].building_nm == "Building 01"
    assert result[0].line_nm == "Line 01"
    assert result[0].down_time_target == 100.0
    assert result[0].down_time_value == 95.0


@patch("app.services.downtime_line.get_session")
@pytest.mark.asyncio
async def test_fetch_downtime_line_with_date(mock_get_session, mock_downtime_line_rows):
    """fetch_downtime_line - date filtering test"""
    from app.services.downtime_line import fetch_downtime_line

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = [mock_downtime_line_rows[0]]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_downtime_line(line_cd="LINE001", date=date(2024, 1, 15))

    # Validate
    assert len(result) == 1
    assert result[0].business_date == date(2024, 1, 15)


# ==================== Downtime Machine service function test ====================

@patch("app.services.downtime_machine.get_session")
@pytest.mark.asyncio
async def test_fetch_downtime_machine(mock_get_session, mock_downtime_machine_rows):
    """fetch_downtime_machine service function test"""
    from app.services.downtime_machine import fetch_downtime_machine

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = mock_downtime_machine_rows
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_downtime_machine(machine_cd="MCA02")

    # Validate
    assert len(result) == 2
    assert isinstance(result[0], DowntimeMachineRecord)
    assert result[0].machine_cd == "MCA02"
    assert result[0].line_cd == "LINE001"
    assert result[0].line_nm == "Line 01"
    assert result[0].factory == "F001"
    assert result[0].factory_nm == "Factory 01"
    assert result[0].building == "B001"
    assert result[0].building_nm == "Building 01"
    assert result[0].mes_machine_nm == "Machine 02"
    assert result[0].down_time_target == 100.0
    assert result[0].down_time_value == 95.0


@patch("app.services.downtime_machine.get_session")
@pytest.mark.asyncio
async def test_fetch_downtime_machine_with_date(mock_get_session, mock_downtime_machine_rows):
    """fetch_downtime_machine - date filtering test"""
    from app.services.downtime_machine import fetch_downtime_machine

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = [mock_downtime_machine_rows[0]]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_downtime_machine(machine_cd="MCA02", date=date(2024, 1, 15))

    # Validate
    assert len(result) == 1
    assert result[0].business_date == date(2024, 1, 15)


# ==================== Downtime Line/Machine schema validation test ====================

def test_downtime_line_record_schema():
    """DowntimeLineRecord schema validation"""
    record = DowntimeLineRecord(
        business_date=date(2024, 1, 15),
        factory="F001",
        factory_nm="Factory 01",
        building="B001",
        building_nm="Building 01",
        line_cd="LINE001",
        line_nm="Line 01",
        down_time_target=100.0,
        down_time_value=95.0,
    )

    assert record.business_date == date(2024, 1, 15)
    assert record.line_cd == "LINE001"
    assert record.line_nm == "Line 01"
    assert record.factory_nm == "Factory 01"
    assert record.building_nm == "Building 01"
    assert record.down_time_target == 100.0
    assert record.down_time_value == 95.0


def test_downtime_line_response_schema():
    """DowntimeLineResponse schema validation"""
    records = [
        DowntimeLineRecord(
            business_date=date(2024, 1, 15),
            factory="F001",
            factory_nm="Factory 01",
            building="B001",
            building_nm="Building 01",
            line_cd="LINE001",
            line_nm="Line 01",
            down_time_target=100.0,
            down_time_value=95.0,
        ),
    ]
    response = DowntimeLineResponse(data=records, total=1)

    assert response.total == 1
    assert len(response.data) == 1
    assert response.data[0].line_cd == "LINE001"
    assert response.data[0].line_nm == "Line 01"


def test_downtime_machine_record_schema():
    """DowntimeMachineRecord schema validation"""
    record = DowntimeMachineRecord(
        business_date=date(2024, 1, 15),
        factory="F001",
        factory_nm="Factory 01",
        building="B001",
        building_nm="Building 01",
        line_cd="LINE001",
        line_nm="Line 01",
        machine_cd="MCA02",
        mes_machine_nm="Machine 02",
        down_time_target=100.0,
        down_time_value=95.0,
    )

    assert record.business_date == date(2024, 1, 15)
    assert record.machine_cd == "MCA02"
    assert record.line_nm == "Line 01"
    assert record.factory_nm == "Factory 01"
    assert record.building_nm == "Building 01"
    assert record.mes_machine_nm == "Machine 02"
    assert record.down_time_target == 100.0


def test_downtime_machine_response_schema():
    """DowntimeMachineResponse schema validation"""
    records = [
        DowntimeMachineRecord(
            business_date=date(2024, 1, 15),
            factory="F001",
            factory_nm="Factory 01",
            building="B001",
            building_nm="Building 01",
            line_cd="LINE001",
            line_nm="Line 01",
            machine_cd="MCA02",
            mes_machine_nm="Machine 02",
            down_time_target=100.0,
            down_time_value=95.0,
        ),
    ]
    response = DowntimeMachineResponse(data=records, total=1)

    assert response.total == 1
    assert len(response.data) == 1
    assert response.data[0].machine_cd == "MCA02"
    assert response.data[0].line_nm == "Line 01"


# ==================== Downtime Line/Machine API endpoint test ====================

@pytest.mark.asyncio
async def test_get_downtime_line_endpoint():
    """GET /api/v1/downtime/line endpoint test"""
    # Prevent Oracle initialization before router import
    if "CMMS_DATABASE_URL" in os.environ:
        del os.environ["CMMS_DATABASE_URL"]
    
    from fastapi import FastAPI
    from app.api.v1.downtime import router
    
    # Create independent FastAPI app
    test_app = FastAPI()
    test_app.include_router(router, prefix="/api/v1")
    from fastapi.testclient import TestClient
    client = TestClient(test_app)
    
    # Mock setting
    mock_records = [
        DowntimeLineRecord(
            business_date=date(2024, 1, 15),
            factory="F001",
            factory_nm="Factory 01",
            building="B001",
            building_nm="Building 01",
            line_cd="LINE001",
            line_nm="Line 01",
            down_time_target=100.0,
            down_time_value=95.0,
        ),
    ]
    
    with patch("app.api.v1.downtime.fetch_downtime_line", new_callable=AsyncMock, return_value=mock_records):
        # API call
        response = client.get("/api/v1/downtime/line?line_cd=LINE001")

        # Validate
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert len(data["data"]) == 1
        assert data["data"][0]["line_cd"] == "LINE001"
        assert data["data"][0]["line_nm"] == "Line 01"
        assert data["data"][0]["factory_nm"] == "Factory 01"
        assert data["data"][0]["building_nm"] == "Building 01"
        assert data["data"][0]["down_time_target"] == 100.0


@pytest.mark.asyncio
async def test_get_downtime_line_with_date():
    """GET /api/v1/downtime/line - date parameter test"""
    # Prevent Oracle initialization before router import
    if "CMMS_DATABASE_URL" in os.environ:
        del os.environ["CMMS_DATABASE_URL"]
    
    from fastapi import FastAPI
    from app.api.v1.downtime import router
    
    # Create independent FastAPI app
    test_app = FastAPI()
    test_app.include_router(router, prefix="/api/v1")
    from fastapi.testclient import TestClient
    client = TestClient(test_app)
    
    # Mock setting
    mock_records = [
        DowntimeLineRecord(
            business_date=date(2024, 1, 15),
            factory="F001",
            factory_nm="Factory 01",
            building="B001",
            building_nm="Building 01",
            line_cd="LINE001",
            line_nm="Line 01",
            down_time_target=100.0,
            down_time_value=95.0,
        ),
    ]
    
    mock_fetch = patch("app.api.v1.downtime.fetch_downtime_line", new_callable=AsyncMock, return_value=mock_records)
    with mock_fetch as mock:
        # API call - yyyyMMdd format
        response = client.get("/api/v1/downtime/line?line_cd=LINE001&date=20240115")

        # Validate
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        # Validate if date parameter is correctly parsed
        call_args = mock.call_args
        assert call_args.kwargs["date"] == date(2024, 1, 15)


@pytest.mark.asyncio
async def test_get_downtime_machine_endpoint():
    """GET /api/v1/downtime/machine endpoint test"""
    # Prevent Oracle initialization before router import
    if "CMMS_DATABASE_URL" in os.environ:
        del os.environ["CMMS_DATABASE_URL"]
    
    from fastapi import FastAPI
    from app.api.v1.downtime import router
    
    # Create independent FastAPI app
    test_app = FastAPI()
    test_app.include_router(router, prefix="/api/v1")
    from fastapi.testclient import TestClient
    client = TestClient(test_app)
    
    # Mock setting
    mock_records = [
        DowntimeMachineRecord(
            business_date=date(2024, 1, 15),
            factory="F001",
            factory_nm="Factory 01",
            building="B001",
            building_nm="Building 01",
            line_cd="LINE001",
            line_nm="Line 01",
            machine_cd="MCA02",
            mes_machine_nm="Machine 02",
            down_time_target=100.0,
            down_time_value=95.0,
        ),
    ]
    
    with patch("app.api.v1.downtime.fetch_downtime_machine", new_callable=AsyncMock, return_value=mock_records):
        # API call
        response = client.get("/api/v1/downtime/machine?machine_cd=MCA02")

        # Validate
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert len(data["data"]) == 1
        assert data["data"][0]["machine_cd"] == "MCA02"
        assert data["data"][0]["line_nm"] == "Line 01"
        assert data["data"][0]["factory_nm"] == "Factory 01"
        assert data["data"][0]["building_nm"] == "Building 01"
        assert data["data"][0]["mes_machine_nm"] == "Machine 02"
        assert data["data"][0]["down_time_target"] == 100.0


@pytest.mark.asyncio
async def test_get_downtime_machine_with_date():
    """GET /api/v1/downtime/machine - date parameter test"""
    # Prevent Oracle initialization before router import
    if "CMMS_DATABASE_URL" in os.environ:
        del os.environ["CMMS_DATABASE_URL"]
    
    from fastapi import FastAPI
    from app.api.v1.downtime import router
    
    # Create independent FastAPI app
    test_app = FastAPI()
    test_app.include_router(router, prefix="/api/v1")
    from fastapi.testclient import TestClient
    client = TestClient(test_app)
    
    # Mock setting
    mock_records = [
        DowntimeMachineRecord(
            business_date=date(2024, 1, 15),
            factory="F001",
            factory_nm="Factory 01",
            building="B001",
            building_nm="Building 01",
            line_cd="LINE001",
            line_nm="Line 01",
            machine_cd="MCA02",
            mes_machine_nm="Machine 02",
            down_time_target=100.0,
            down_time_value=95.0,
        ),
    ]
    
    mock_fetch = patch("app.api.v1.downtime.fetch_downtime_machine", new_callable=AsyncMock, return_value=mock_records)
    with mock_fetch as mock:
        # API call - yyyy-MM-dd format
        response = client.get("/api/v1/downtime/machine?machine_cd=MCA02&date=2024-01-15")

        # Validate
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        # Validate if date parameter is correctly parsed
        call_args = mock.call_args
        assert call_args.kwargs["date"] == date(2024, 1, 15)


@pytest.mark.asyncio
async def test_get_downtime_line_invalid_date():
    """GET /api/v1/downtime/line - invalid date format test"""
    # Prevent Oracle initialization before router import
    if "CMMS_DATABASE_URL" in os.environ:
        del os.environ["CMMS_DATABASE_URL"]
    
    from fastapi import FastAPI
    from app.api.v1.downtime import router
    
    # Create independent FastAPI app
    test_app = FastAPI()
    test_app.include_router(router, prefix="/api/v1")
    from fastapi.testclient import TestClient
    client = TestClient(test_app)
    
    # API call - invalid date format
    response = client.get("/api/v1/downtime/line?line_cd=LINE001&date=invalid")

    # Validate
    assert response.status_code == 400
    data = response.json()
    assert "detail" in data
    assert "Invalid date format" in data["detail"]


@pytest.mark.asyncio
async def test_get_downtime_machine_invalid_date():
    """GET /api/v1/downtime/machine - invalid date format test"""
    # Prevent Oracle initialization before router import
    if "CMMS_DATABASE_URL" in os.environ:
        del os.environ["CMMS_DATABASE_URL"]
    
    from fastapi import FastAPI
    from app.api.v1.downtime import router
    
    # Create independent FastAPI app
    test_app = FastAPI()
    test_app.include_router(router, prefix="/api/v1")
    from fastapi.testclient import TestClient
    client = TestClient(test_app)
    
    # API call - invalid date format
    response = client.get("/api/v1/downtime/machine?machine_cd=MCA02&date=invalid")

    # Validate
    assert response.status_code == 400
    data = response.json()
    assert "detail" in data
    assert "Invalid date format" in data["detail"]

