"""Tests for Work Order analysis endpoints."""

import os
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime

# Test environment variable setting (dummy values, actual DB connection is replaced by mocking)
# conftest.py fixture runs first, but set explicitly to be sure
os.environ.setdefault("MONTRG_DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test_db")

from app.main import app
from app.schemas.wo_analysis import (
    ProcessWoStatus,
    OperationWoStatus,
    LineWoStatus,
    MachineWoStatus,
    WoStatusCount,
)

client = TestClient(app)


# Mock data helper function
def create_mock_row(**kwargs):
    """Create mock row object"""
    row = MagicMock()
    for key, value in kwargs.items():
        setattr(row, key, value)
    return row


@pytest.fixture
def mock_db_rows_machine():
    """machine group_by mock data"""
    return [
        create_mock_row(
            process_name="PROCESS01",
            op_cd="OP01",
            line_no=1,
            machine_cd="MCA34",
            wo_status="COMPLETED",
            count=150,
        ),
        create_mock_row(
            process_name="PROCESS01",
            op_cd="OP01",
            line_no=1,
            machine_cd="MCA34",
            wo_status="PENDING",
            count=10,
        ),
        create_mock_row(
            process_name="PROCESS01",
            op_cd="OP01",
            line_no=2,
            machine_cd="MCA35",
            wo_status="COMPLETED",
            count=80,
        ),
    ]


@pytest.fixture
def mock_db_rows_op_line():
    """op_line group_by mock data"""
    return [
        create_mock_row(
            process_name="PROCESS01",
            op_cd="OP01",
            line_no=1,
            wo_status="COMPLETED",
            count=200,
        ),
        create_mock_row(
            process_name="PROCESS01",
            op_cd="OP01",
            line_no=2,
            wo_status="PENDING",
            count=30,
        ),
    ]


@pytest.fixture
def mock_db_rows_op():
    """op group_by mock data"""
    return [
        create_mock_row(
            process_name="PROCESS01",
            op_cd="OP01",
            wo_status="COMPLETED",
            count=300,
        ),
        create_mock_row(
            process_name="PROCESS01",
            op_cd="OP02",
            wo_status="PENDING",
            count=50,
        ),
    ]


@pytest.fixture
def mock_db_rows_process():
    """process group_by mock data"""
    return [
        create_mock_row(
            process_name="PROCESS01",
            wo_status="COMPLETED",
            count=500,
        ),
        create_mock_row(
            process_name="PROCESS02",
            wo_status="PENDING",
            count=100,
        ),
    ]


@patch("app.services.wo_analysis._execute_query", new_callable=AsyncMock)
@pytest.mark.asyncio
async def test_analyze_wo_by_machine(mock_execute, mock_db_rows_machine):
    """machine group_by test"""
    from app.services.wo_analysis import analyze_wo_by_machine
    
    mock_execute.return_value = mock_db_rows_machine
    
    result = await analyze_wo_by_machine("2025-11-01", "2025-11-30")
    
    assert "PROCESS01" in result
    assert "OP01" in result["PROCESS01"].operations
    assert 1 in result["PROCESS01"].operations["OP01"].lines
    assert "MCA34" in result["PROCESS01"].operations["OP01"].lines[1].machines
    assert "COMPLETED" in result["PROCESS01"].operations["OP01"].lines[1].machines["MCA34"].wo_statuses
    assert result["PROCESS01"].operations["OP01"].lines[1].machines["MCA34"].wo_statuses["COMPLETED"].count == 150


@patch("app.services.wo_analysis._execute_query", new_callable=AsyncMock)
@pytest.mark.asyncio
async def test_analyze_wo_by_op_line(mock_execute, mock_db_rows_op_line):
    """op_line group_by test"""
    from app.services.wo_analysis import analyze_wo_by_op_line
    
    mock_execute.return_value = mock_db_rows_op_line
    
    result = await analyze_wo_by_op_line("2025-11-01", "2025-11-30")
    
    assert "PROCESS01" in result
    assert "OP01" in result["PROCESS01"].operations
    assert 1 in result["PROCESS01"].operations["OP01"].lines
    assert "COMPLETED" in result["PROCESS01"].operations["OP01"].lines[1].wo_statuses
    assert result["PROCESS01"].operations["OP01"].lines[1].wo_statuses["COMPLETED"].count == 200


@patch("app.services.wo_analysis._execute_query", new_callable=AsyncMock)
@pytest.mark.asyncio
async def test_analyze_wo_by_op(mock_execute, mock_db_rows_op):
    """op group_by test"""
    from app.services.wo_analysis import analyze_wo_by_op
    
    mock_execute.return_value = mock_db_rows_op
    
    result = await analyze_wo_by_op("2025-11-01", "2025-11-30")
    
    assert "PROCESS01" in result
    assert "OP01" in result["PROCESS01"].operations
    assert "COMPLETED" in result["PROCESS01"].operations["OP01"].wo_statuses
    assert result["PROCESS01"].operations["OP01"].wo_statuses["COMPLETED"].count == 300


@patch("app.services.wo_analysis._execute_query", new_callable=AsyncMock)
@pytest.mark.asyncio
async def test_analyze_wo_by_process(mock_execute, mock_db_rows_process):
    """process group_by test"""
    from app.services.wo_analysis import analyze_wo_by_process
    
    mock_execute.return_value = mock_db_rows_process
    
    result = await analyze_wo_by_process("2025-11-01", "2025-11-30")
    
    assert "PROCESS01" in result
    assert "COMPLETED" in result["PROCESS01"].wo_statuses
    assert result["PROCESS01"].wo_statuses["COMPLETED"].count == 500


@patch("app.api.v1.wo_analysis.analyze_wo_by_machine", new_callable=AsyncMock)
def test_api_endpoint_machine(mock_analyze):
    """API endpoint test - machine"""
    mock_result = {
        "PROCESS01": ProcessWoStatus(
            operations={
                "OP01": OperationWoStatus(
                    lines={
                        1: LineWoStatus(
                            machines={
                                "MCA34": MachineWoStatus(
                                    wo_statuses={
                                        "COMPLETED": WoStatusCount(wo_status="COMPLETED", count=150)
                                    }
                                )
                            }
                        )
                    }
                )
            }
        )
    }
    async def mock_analyze_func(*args, **kwargs):
        return mock_result
    mock_analyze.side_effect = mock_analyze_func
    
    response = client.get(
        "/api/v1/wo-analysis",
        params={
            "start_date": "2025-11-01",
            "end_date": "2025-11-30",
            "group_by": "machine",
        },
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "processes" in data
    assert "PROCESS01" in data["processes"]
    assert "OP01" in data["processes"]["PROCESS01"]["operations"]


@patch("app.api.v1.wo_analysis.analyze_wo_by_op_line", new_callable=AsyncMock)
def test_api_endpoint_op_line(mock_analyze):
    """API endpoint test - op_line"""
    mock_result = {
        "PROCESS01": ProcessWoStatus(
            operations={
                "OP01": OperationWoStatus(
                    lines={
                        1: LineWoStatus(
                            wo_statuses={
                                "COMPLETED": WoStatusCount(wo_status="COMPLETED", count=200)
                            }
                        )
                    }
                )
            }
        )
    }
    async def mock_analyze_func(*args, **kwargs):
        return mock_result
    mock_analyze.side_effect = mock_analyze_func
    
    response = client.get(
        "/api/v1/wo-analysis",
        params={
            "start_date": "2025-11-01",
            "end_date": "2025-11-30",
            "group_by": "op_line",
        },
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "processes" in data
    assert "PROCESS01" in data["processes"]


@patch("app.api.v1.wo_analysis.analyze_wo_by_op", new_callable=AsyncMock)
def test_api_endpoint_op(mock_analyze):
    """API endpoint test - op"""
    mock_result = {
        "PROCESS01": ProcessWoStatus(
            operations={
                "OP01": OperationWoStatus(
                    wo_statuses={
                        "COMPLETED": WoStatusCount(wo_status="COMPLETED", count=300)
                    }
                )
            }
        )
    }
    async def mock_analyze_func(*args, **kwargs):
        return mock_result
    mock_analyze.side_effect = mock_analyze_func
    
    response = client.get(
        "/api/v1/wo-analysis",
        params={
            "start_date": "2025-11-01",
            "end_date": "2025-11-30",
            "group_by": "op",
        },
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "processes" in data


@patch("app.api.v1.wo_analysis.analyze_wo_by_process", new_callable=AsyncMock)
def test_api_endpoint_process(mock_analyze):
    """API endpoint test - process"""
    mock_result = {
        "PROCESS01": ProcessWoStatus(
            wo_statuses={
                "COMPLETED": WoStatusCount(wo_status="COMPLETED", count=500)
            }
        )
    }
    async def mock_analyze_func(*args, **kwargs):
        return mock_result
    mock_analyze.side_effect = mock_analyze_func
    
    response = client.get(
        "/api/v1/wo-analysis",
        params={
            "start_date": "2025-11-01",
            "end_date": "2025-11-30",
            "group_by": "process",
        },
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "processes" in data
    assert "COMPLETED" in data["processes"]["PROCESS01"]["wo_statuses"]


def test_api_endpoint_invalid_date_format():
    """Invalid date format test"""
    response = client.get(
        "/api/v1/wo-analysis",
        params={
            "start_date": "2025/11/01",  # Invalid format
            "end_date": "2025-11-30",
            "group_by": "op",
        },
    )
    
    assert response.status_code == 422  # Validation error


def test_api_endpoint_date_range_validation():
    """Date range validation test"""
    response = client.get(
        "/api/v1/wo-analysis",
        params={
            "start_date": "2025-11-30",
            "end_date": "2025-11-01",  # Start date is later than end date
            "group_by": "op",
        },
    )
    
    # API validates, so should return 400
    assert response.status_code == 400


def test_api_endpoint_invalid_group_by():
    """Invalid group_by value test"""
    response = client.get(
        "/api/v1/wo-analysis",
        params={
            "start_date": "2025-11-01",
            "end_date": "2025-11-30",
            "group_by": "invalid",  # Invalid value
        },
    )
    
    assert response.status_code == 422  # Validation error (regex check)


def test_get_query_by_group():
    """Query generation function test"""
    from app.services.wo_analysis import _get_query_by_group
    
    # Test all group_by types
    for group_by in ["machine", "op_line", "op", "process"]:
        query = _get_query_by_group(group_by)
        assert query is not None
        assert isinstance(query, type(_get_query_by_group("machine")))
    
    # Test invalid group_by value
    with pytest.raises(ValueError, match="Invalid group_by value"):
        _get_query_by_group("invalid")


def test_build_hierarchical_structure_machine():
    """Hierarchical structure conversion test - machine"""
    from app.services.wo_analysis import _build_hierarchical_structure
    
    rows = [
        create_mock_row(
            process_name="PROCESS01",
            op_cd="OP01",
            line_no=1,
            machine_cd="MCA34",
            wo_status="COMPLETED",
            count=150,
        ),
    ]
    
    result = _build_hierarchical_structure(rows, "machine")
    
    assert "PROCESS01" in result
    assert "OP01" in result["PROCESS01"].operations
    assert 1 in result["PROCESS01"].operations["OP01"].lines
    assert "MCA34" in result["PROCESS01"].operations["OP01"].lines[1].machines
    assert result["PROCESS01"].operations["OP01"].lines[1].machines["MCA34"].wo_statuses["COMPLETED"].count == 150


def test_build_hierarchical_structure_process():
    """Hierarchical structure conversion test - process"""
    from app.services.wo_analysis import _build_hierarchical_structure
    
    rows = [
        create_mock_row(
            process_name="PROCESS01",
            wo_status="COMPLETED",
            count=500,
        ),
    ]
    
    result = _build_hierarchical_structure(rows, "process")
    
    assert "PROCESS01" in result
    assert "COMPLETED" in result["PROCESS01"].wo_statuses
    assert result["PROCESS01"].wo_statuses["COMPLETED"].count == 500

