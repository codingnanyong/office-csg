"""Service layer for Machine Grade data operations."""

from datetime import date
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session
from app.models.machine_grade import MachineGrade


async def fetch_machine_grade(
    mc_nm: Optional[str] = None,
    ym: Optional[date] = None,
    grade: Optional[str] = None,
) -> List[MachineGrade]:
    """Machine Grade data from gold.machine_grade table
    
    Args:
        mc_nm: filter by machine name (optional, if not provided, all machines)
        ym: filter by year month (optional, if not provided, all year months)
        grade: filter by grade (optional, if not provided, all grades)
    
    Returns:
        Machine Grade record list
    """
    async with get_session(db_alias="montrg") as session:
        query = select(MachineGrade)
        
        # add filter conditions
        if mc_nm:
            query = query.where(MachineGrade.mc_nm == mc_nm)
        if ym:
            query = query.where(MachineGrade.ym == ym)
        if grade:
            query = query.where(MachineGrade.grade == grade)
        
        result = await session.execute(query)
        return list(result.scalars().all())

