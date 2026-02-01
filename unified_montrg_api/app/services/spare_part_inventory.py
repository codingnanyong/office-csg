"""Service layer for Spare Part Inventory data operations."""

from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session
from app.models.spare_part_inventory import SparePartInventory


async def fetch_spare_part_inventory(
    mach_group: Optional[str] = None,
    part_cd: Optional[str] = None,
    part_nm: Optional[str] = None,
) -> List[SparePartInventory]:
    """Fetch Spare Part Inventory data from spare_part_inventory table
    
    Args:
        mach_group: filter by machine group (optional, if not provided, all machine groups)
        part_cd: filter by part code (optional, if not provided, all part codes)
        part_nm: filter by part name (optional, if not provided, all part names)
    
    Returns:
        Spare Part Inventory record list
    """
    async with get_session(db_alias="montrg") as session:
        query = select(SparePartInventory)
        
        # add filter conditions
        if mach_group:
            query = query.where(SparePartInventory.mach_group == mach_group)
        if part_cd:
            query = query.where(SparePartInventory.part_cd == part_cd)
        if part_nm:
            # use like() instead of contains() for PostgreSQL Foreign Table
            query = query.where(SparePartInventory.part_nm.like(f"%{part_nm}%"))
        
        result = await session.execute(query)
        return list(result.scalars().all())
