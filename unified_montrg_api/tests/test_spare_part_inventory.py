"""Tests for Spare Part Inventory service."""

import os
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

# Test environment variable setting
os.environ.setdefault("MONTRG_DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test_db")

# Prevent Oracle initialization error
if "CMMS_DATABASE_URL" in os.environ:
    del os.environ["CMMS_DATABASE_URL"]

from app.models.spare_part_inventory import SparePartInventory


@pytest.fixture
def mock_spare_part_inventory_objects():
    """Mock object for Spare Part Inventory"""
    return [
        SparePartInventory(
            mach_group="Phylon",
            part_cd="PART001",
            part_nm="Bearing Assembly",
            stock=10,
            mach_id="MACHINE01",
            last_rep_dt="20241201",
            baseline_stock=15,
            exp_cycle=12,
            exp_rep_dt="20250301",
            exp_orde_dt="20250215",
        ),
        SparePartInventory(
            mach_group="Phylon",
            part_cd="PART002",
            part_nm="Seal Ring",
            stock=5,
            mach_id="MACHINE02",
            last_rep_dt="20241115",
            baseline_stock=10,
            exp_cycle=8,
            exp_rep_dt="20250220",
            exp_orde_dt="20250210",
        ),
        SparePartInventory(
            mach_group="Injection Pressing",
            part_cd="PART003",
            part_nm="Hydraulic Pump",
            stock=3,
            mach_id="MACHINE03",
            last_rep_dt="20241020",
            baseline_stock=8,
            exp_cycle=6,
            exp_rep_dt="20250115",
            exp_orde_dt="20250105",
        ),
    ]


# ==================== Service function test ====================

@patch("app.services.spare_part_inventory.get_session")
@pytest.mark.asyncio
async def test_fetch_spare_part_inventory_all(mock_get_session, mock_spare_part_inventory_objects):
    """fetch_spare_part_inventory - query all test"""
    from app.services.spare_part_inventory import fetch_spare_part_inventory

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = mock_spare_part_inventory_objects
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_spare_part_inventory()

    # Validate
    assert len(result) == 3
    assert isinstance(result[0], SparePartInventory)
    assert result[0].mach_group == "Phylon"
    assert result[0].part_cd == "PART001"
    assert result[0].stock == 10


@patch("app.services.spare_part_inventory.get_session")
@pytest.mark.asyncio
async def test_fetch_spare_part_inventory_by_mach_group(mock_get_session, mock_spare_part_inventory_objects):
    """fetch_spare_part_inventory - mach_group filter test"""
    from app.services.spare_part_inventory import fetch_spare_part_inventory

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    # Filtered result by mach_group (returns only Phylon group)
    filtered_objects = [obj for obj in mock_spare_part_inventory_objects if obj.mach_group == "Phylon"]
    mock_result.scalars.return_value.all.return_value = filtered_objects
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_spare_part_inventory(mach_group="Phylon")

    # Validate
    assert len(result) == 2
    assert all(r.mach_group == "Phylon" for r in result)


@patch("app.services.spare_part_inventory.get_session")
@pytest.mark.asyncio
async def test_fetch_spare_part_inventory_by_part_cd(mock_get_session, mock_spare_part_inventory_objects):
    """fetch_spare_part_inventory - part_cd filter test"""
    from app.services.spare_part_inventory import fetch_spare_part_inventory

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    # Filtered result by part_cd (returns only one)
    mock_result.scalars.return_value.all.return_value = [mock_spare_part_inventory_objects[0]]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_spare_part_inventory(part_cd="PART001")

    # Validate
    assert len(result) == 1
    assert result[0].part_cd == "PART001"


@patch("app.services.spare_part_inventory.get_session")
@pytest.mark.asyncio
async def test_fetch_spare_part_inventory_by_part_nm(mock_get_session, mock_spare_part_inventory_objects):
    """fetch_spare_part_inventory - part_nm filter test"""
    from app.services.spare_part_inventory import fetch_spare_part_inventory

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    # Filtered result by part_nm (returns items containing "Bearing")
    filtered_objects = [obj for obj in mock_spare_part_inventory_objects if "Bearing" in obj.part_nm]
    mock_result.scalars.return_value.all.return_value = filtered_objects
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_spare_part_inventory(part_nm="Bearing")

    # Validate
    assert len(result) == 1
    assert "Bearing" in result[0].part_nm


@patch("app.services.spare_part_inventory.get_session")
@pytest.mark.asyncio
async def test_fetch_spare_part_inventory_multiple_filters(mock_get_session, mock_spare_part_inventory_objects):
    """fetch_spare_part_inventory - multiple filter combination test"""
    from app.services.spare_part_inventory import fetch_spare_part_inventory

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [mock_spare_part_inventory_objects[0]]
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_spare_part_inventory(
        mach_group="Phylon",
        part_cd="PART001",
        part_nm="Bearing"
    )

    # Validate
    assert len(result) == 1
    assert result[0].mach_group == "Phylon"
    assert result[0].part_cd == "PART001"
    assert "Bearing" in result[0].part_nm


@patch("app.services.spare_part_inventory.get_session")
@pytest.mark.asyncio
async def test_fetch_spare_part_inventory_empty_result(mock_get_session):
    """fetch_spare_part_inventory - empty result test"""
    from app.services.spare_part_inventory import fetch_spare_part_inventory

    # Mock session setting
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Call service function
    result = await fetch_spare_part_inventory(mach_group="NONEXISTENT")

    # Validate
    assert len(result) == 0
