"""Tests for Machine Grade service."""

import os
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import date
from decimal import Decimal

# Test environment variable setting
os.environ.setdefault("MONTRG_DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test_db")

# Prevent Oracle initialization error
if "CMMS_DATABASE_URL" in os.environ:
    del os.environ["CMMS_DATABASE_URL"]

from app.models.machine_grade import MachineGrade


@pytest.fixture
def mock_machine_grade_objects():
    """Mock object for Machine Grade"""
    return [
        MachineGrade(
            mc_nm="MACHINE01",
            ym=date(2024, 1, 1),
            failures_12m=5,
            operation_time=7200,
            repair_time=120,
            operation_time_day=Decimal("5.0"),
            mtbf_min=Decimal("1440.0"),
            mttr_min=Decimal("24.0"),
            availability=Decimal("0.9833"),
            machine_group="Injection Pressing",
            mtbf_z=Decimal("1.5"),
            mttr_z=Decimal("-0.5"),
            grade="A",
            comment="Excellent performance",
            pm_cycle_week=8,
        ),
        MachineGrade(
            mc_nm="MACHINE02",
            ym=date(2024, 1, 1),
            failures_12m=10,
            operation_time=7000,
            repair_time=200,
            operation_time_day=Decimal("4.86"),
            mtbf_min=Decimal("700.0"),
            mttr_min=Decimal("20.0"),
            availability=Decimal("0.9722"),
            machine_group="Injection Pressing",
            mtbf_z=Decimal("0.2"),
            mttr_z=Decimal("0.3"),
            grade="B",
            comment=None,
            pm_cycle_week=6,
        ),
    ]


# ==================== Service function test ====================

@patch("app.services.machine_grade.get_session")
@pytest.mark.asyncio
async def test_fetch_machine_grade_all(mock_get_session, mock_machine_grade_objects):
    """fetch_machine_grade - query all test"""
    from app.services.machine_grade import fetch_machine_grade

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = mock_machine_grade_objects
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_machine_grade()

    # Validate
    assert len(result) == 2
    assert isinstance(result[0], MachineGrade)
    assert result[0].mc_nm == "MACHINE01"
    assert result[0].grade == "A"
    assert result[0].failures_12m == 5


@patch("app.services.machine_grade.get_session")
@pytest.mark.asyncio
async def test_fetch_machine_grade_by_mc_nm(mock_get_session, mock_machine_grade_objects):
    """fetch_machine_grade - mc_nm filter test"""
    from app.services.machine_grade import fetch_machine_grade

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    # Filtered result by mc_nm (returns only one)
    mock_result.scalars.return_value.all.return_value = [mock_machine_grade_objects[0]]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_machine_grade(mc_nm="MACHINE01")

    # Validate
    assert len(result) == 1
    assert result[0].mc_nm == "MACHINE01"


@patch("app.services.machine_grade.get_session")
@pytest.mark.asyncio
async def test_fetch_machine_grade_by_ym(mock_get_session, mock_machine_grade_objects):
    """fetch_machine_grade - ym filter test"""
    from app.services.machine_grade import fetch_machine_grade

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = mock_machine_grade_objects
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_machine_grade(ym=date(2024, 1, 1))

    # Validate
    assert len(result) == 2
    assert all(r.ym == date(2024, 1, 1) for r in result)


@patch("app.services.machine_grade.get_session")
@pytest.mark.asyncio
async def test_fetch_machine_grade_by_grade(mock_get_session, mock_machine_grade_objects):
    """fetch_machine_grade - grade filter test"""
    from app.services.machine_grade import fetch_machine_grade

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    # Filtered result by grade (returns only one)
    mock_result.scalars.return_value.all.return_value = [mock_machine_grade_objects[0]]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_machine_grade(grade="A")

    # Validate
    assert len(result) == 1
    assert result[0].grade == "A"


@patch("app.services.machine_grade.get_session")
@pytest.mark.asyncio
async def test_fetch_machine_grade_multiple_filters(mock_get_session, mock_machine_grade_objects):
    """fetch_machine_grade - multiple filter combination test"""
    from app.services.machine_grade import fetch_machine_grade

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [mock_machine_grade_objects[0]]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_machine_grade(
        mc_nm="MACHINE01",
        ym=date(2024, 1, 1),
        grade="A"
    )

    # Validate
    assert len(result) == 1
    assert result[0].mc_nm == "MACHINE01"
    assert result[0].grade == "A"


@patch("app.services.machine_grade.get_session")
@pytest.mark.asyncio
async def test_fetch_machine_grade_empty_result(mock_get_session):
    """fetch_machine_grade - empty result test"""
    from app.services.machine_grade import fetch_machine_grade

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_machine_grade(mc_nm="NONEXISTENT")

    # Validate
    assert len(result) == 0

