"""Tests for MC Master service."""

import os
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime

# Test environment variable setting
os.environ.setdefault("MONTRG_DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test_db")

# Prevent Oracle initialization error
if "CMMS_DATABASE_URL" in os.environ:
    del os.environ["CMMS_DATABASE_URL"]

from app.models.mc_master import McMaster


@pytest.fixture
def mock_mc_master_objects():
    """Mock object for MC Master"""
    return [
        McMaster(
            machine_cd="MCA01",
            company_name="CompanyA",
            plant_cd="PLANT1",
            process_name="PROCESS1",
            op_cd="OP01",
            line_no=1,
            machine_no=1,
            upd_dt=datetime(2024, 1, 15, 12, 0, 0),
            upd_id="USER01",
        ),
        McMaster(
            machine_cd="MCA02",
            company_name="CompanyA",
            plant_cd="PLANT1",
            process_name="PROCESS1",
            op_cd="OP01",
            line_no=1,
            machine_no=2,
            upd_dt=datetime(2024, 1, 15, 12, 0, 0),
            upd_id="USER01",
        ),
        McMaster(
            machine_cd="MCB01",
            company_name="CompanyA",
            plant_cd="PLANT1",
            process_name="PROCESS1",
            op_cd="OP02",
            line_no=2,
            machine_no=1,
            upd_dt=datetime(2024, 1, 15, 12, 0, 0),
            upd_id="USER01",
        ),
    ]


# ==================== Service function test ====================

@patch("app.services.mc_master.get_session")
@pytest.mark.asyncio
async def test_fetch_mc_masters_all(mock_get_session, mock_mc_master_objects):
    """fetch_mc_masters - query all test"""
    from app.services.mc_master import fetch_mc_masters

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = mock_mc_master_objects
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_mc_masters()

    # Validate
    assert len(result) == 3
    assert isinstance(result[0], McMaster)
    assert result[0].machine_cd == "MCA01"
    assert result[0].company_name == "CompanyA"


@patch("app.services.mc_master.get_session")
@pytest.mark.asyncio
async def test_fetch_mc_masters_by_plant_cd(mock_get_session, mock_mc_master_objects):
    """fetch_mc_masters - plant_cd filter test"""
    from app.services.mc_master import fetch_mc_masters

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = mock_mc_master_objects
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_mc_masters(plant_cd="PLANT1")

    # Validate
    assert len(result) == 3
    assert all(r.plant_cd == "PLANT1" for r in result)


@patch("app.services.mc_master.get_session")
@pytest.mark.asyncio
async def test_fetch_mc_masters_by_process_name(mock_get_session, mock_mc_master_objects):
    """fetch_mc_masters - process_name filter test"""
    from app.services.mc_master import fetch_mc_masters

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = mock_mc_master_objects
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_mc_masters(process_name="PROCESS1")

    # Validate
    assert len(result) == 3
    assert all(r.process_name == "PROCESS1" for r in result)


@patch("app.services.mc_master.get_session")
@pytest.mark.asyncio
async def test_fetch_mc_masters_by_line_no(mock_get_session, mock_mc_master_objects):
    """fetch_mc_masters - line_no filter test"""
    from app.services.mc_master import fetch_mc_masters

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    # Filtered result by line_no (returns only two)
    mock_result.scalars.return_value.all.return_value = mock_mc_master_objects[:2]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_mc_masters(line_no=1)

    # Validate
    assert len(result) == 2
    assert all(r.line_no == 1 for r in result)


@patch("app.services.mc_master.get_session")
@pytest.mark.asyncio
async def test_fetch_mc_masters_by_machine_no(mock_get_session, mock_mc_master_objects):
    """fetch_mc_masters - machine_no filter test"""
    from app.services.mc_master import fetch_mc_masters

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    # Filtered result by machine_no (returns only one)
    mock_result.scalars.return_value.all.return_value = [mock_mc_master_objects[0]]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_mc_masters(machine_no=1)

    # Validate
    assert len(result) == 1
    assert result[0].machine_no == 1


@patch("app.services.mc_master.get_session")
@pytest.mark.asyncio
async def test_fetch_mc_masters_multiple_filters(mock_get_session, mock_mc_master_objects):
    """fetch_mc_masters - multiple filter combination test"""
    from app.services.mc_master import fetch_mc_masters

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [mock_mc_master_objects[0]]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_mc_masters(
        plant_cd="PLANT1",
        process_name="PROCESS1",
        op_cd="OP01",
        line_no=1,
        machine_no=1
    )

    # Validate
    assert len(result) == 1
    assert result[0].machine_cd == "MCA01"


# ==================== helper function test ====================

def test_build_hierarchical_structure(mock_mc_master_objects):
    """build_hierarchical_structure function test"""
    from app.services.mc_master import build_hierarchical_structure

    # Call function
    result = build_hierarchical_structure(mock_mc_master_objects)

    # Validate
    assert "CompanyA" in result
    company = result["CompanyA"]
    assert "PLANT1" in company.plants
    plant = company.plants["PLANT1"]
    assert "PROCESS1" in plant.processes
    process = plant.processes["PROCESS1"]
    assert "OP01" in process.operations
    operation = process.operations["OP01"]
    assert 1 in operation.lines
    line = operation.lines[1]
    assert "MCA01" in line.machines
    assert "MCA02" in line.machines
    assert line.machines["MCA01"].machine_no == 1
    assert line.machines["MCA02"].machine_no == 2


def test_build_hierarchical_structure_empty_list():
    """build_hierarchical_structure - empty list test"""
    from app.services.mc_master import build_hierarchical_structure

    # Call function
    result = build_hierarchical_structure([])

    # Validate
    assert result == {}


def test_build_hierarchical_structure_single_machine():
    """build_hierarchical_structure - single machine test"""
    from app.services.mc_master import build_hierarchical_structure

    single_machine = [
        McMaster(
            machine_cd="MCA01",
            company_name="CompanyA",
            plant_cd="PLANT1",
            process_name="PROCESS1",
            op_cd="OP01",
            line_no=1,
            machine_no=1,
            upd_dt=datetime(2024, 1, 15, 12, 0, 0),
            upd_id="USER01",
        ),
    ]

    # Call function
    result = build_hierarchical_structure(single_machine)

    # Validate
    assert "CompanyA" in result
    assert "PLANT1" in result["CompanyA"].plants
    assert "PROCESS1" in result["CompanyA"].plants["PLANT1"].processes
    assert "OP01" in result["CompanyA"].plants["PLANT1"].processes["PROCESS1"].operations
    assert 1 in result["CompanyA"].plants["PLANT1"].processes["PROCESS1"].operations["OP01"].lines
    assert "MCA01" in result["CompanyA"].plants["PLANT1"].processes["PROCESS1"].operations["OP01"].lines[1].machines

