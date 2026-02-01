"""Service layer for Economin Lifespan data operations."""

from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session
from app.models.economin_lifespan import EconominLifespan


async def fetch_economin_lifespan(
    machine_group: Optional[str] = None,
    use_year: Optional[int] = None,
) -> List[EconominLifespan]:
    """Fetch Economin Lifespan data from mart_economin_lifespan table
    
    Args:
        machine_group: filter by machine group (optional, if not provided, all machine groups)
        use_year: filter by use year (optional, if not provided, all use years)
    
    Returns:
        Economin Lifespan record list
    """
    async with get_session(db_alias="montrg") as session:
        query = select(EconominLifespan)
        
        # add filter conditions
        if machine_group:
            query = query.where(EconominLifespan.machine_group == machine_group)
        if use_year is not None:
            query = query.where(EconominLifespan.use_year == use_year)
        
        result = await session.execute(query)
        return list(result.scalars().all())

