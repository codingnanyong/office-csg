"""Service layer for Productivity PH Zone operations."""

from datetime import date
from typing import List, Optional
from sqlalchemy import Date, cast, func, select
from app.core.database import get_session
from app.models.mart_productivity_by_ph_zone import MartProductivityByPhZone
from app.models.mart_realtime_productivity_ph_line import MartRealtimeProductivityPhLine
from app.schemas.productivity_ph_zone import ProductivityPhZoneRecord
from app.services.productivity_shift import get_jakarta_shift_business_date


async def fetch_productivity_ph_zone(
    line_group: Optional[str] = None,
    date: Optional[date] = None,
) -> List[ProductivityPhZoneRecord]:
    """Productivity PH Zone data from mart_productivity_by_ph_zone table
    
    Args:
        line_group: filter by line group (optional, if not provided, all line groups)
        date: filter by business date (optional, if not provided, all dates)
    
    Returns:
        Productivity PH Zone record list
    
    Note:
        - silver.mart_productivity_by_ph_zone table
    """
    async with get_session(db_alias="montrg") as session:
        if date:
            query = select(
                MartProductivityByPhZone.business_date,
                MartProductivityByPhZone.line_group,
                MartProductivityByPhZone.plan_qty,
                MartProductivityByPhZone.prod_qty,
                MartProductivityByPhZone.defect_qty,
                MartProductivityByPhZone.quality_rate,
            )

            if line_group:
                # filter by line group (case insensitive)
                query = query.where(func.upper(MartProductivityByPhZone.line_group) == func.upper(line_group))

            query = query.where(MartProductivityByPhZone.business_date == date)
            query = query.order_by(MartProductivityByPhZone.business_date.desc())
        else:
            business_date = get_jakarta_shift_business_date()
            query = select(
                func.cast(business_date, Date).label("business_date"),
                MartRealtimeProductivityPhLine.line_cd,
                MartRealtimeProductivityPhLine.plan_qty,
                MartRealtimeProductivityPhLine.prod_qty,
                MartRealtimeProductivityPhLine.defect_qty,
                MartRealtimeProductivityPhLine.defect_rate,
            )

            if line_group:
                # filter by line code (case insensitive)
                query = query.where(func.upper(MartRealtimeProductivityPhLine.line_cd) == func.upper(line_group))

            query = query.order_by(MartRealtimeProductivityPhLine.line_cd)

        result = await session.execute(query)
        rows = result.all()

        records = []
        for row in rows:
            records.append(
                ProductivityPhZoneRecord(
                    business_date=row[0],
                    line_group=row[1],
                    plan_qty=float(row[2]) if row[2] is not None else None,
                    prod_qty=float(row[3]) if row[3] is not None else None,
                    defect_qty=float(row[4]) if row[4] is not None else None,
                    quality_rate=float(row[5]) if row[5] is not None else None,
                )
            )

        return records

