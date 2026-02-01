"""Service layer for Productivity IP Zone operations."""

from datetime import date
from typing import List, Optional
from sqlalchemy import Date, cast, func, select
from app.core.database import get_session
from app.models.mart_productivity_by_ip_zone import MartProductivityByIpZone
from app.models.mart_realtime_productivity_ip_zone import MartRealtimeProductivityIpZone
from app.schemas.productivity_ip_zone import ProductivityIpZoneRecord
from app.services.productivity_shift import get_jakarta_shift_business_date


async def fetch_productivity_ip_zone(
    zone_cd: Optional[str] = None,
    date: Optional[date] = None,
) -> List[ProductivityIpZoneRecord]:
    """Fetch Productivity IP Zone data from mart_productivity_by_ip_zone table
    
    Args:
        zone_cd: filter by zone code (optional, if not provided, all zones)
        date: filter by business date (optional, if not provided, all dates)
    
    Returns:
        Productivity IP Zone record list
    
    Note:
        - silver.mart_productivity_by_ip_zone table
    """
    async with get_session(db_alias="montrg") as session:
        if date:
            query = select(
                MartProductivityByIpZone.business_date,
                MartProductivityByIpZone.zone_cd,
                MartProductivityByIpZone.plan_qty,
                MartProductivityByIpZone.prod_qty,
                MartProductivityByIpZone.defect_qty,
                MartProductivityByIpZone.quality_rate,
            )

            if zone_cd:
                # filter by zone code (case insensitive)
                query = query.where(func.upper(MartProductivityByIpZone.zone_cd) == func.upper(zone_cd))

            query = query.where(MartProductivityByIpZone.business_date == date)
            query = query.order_by(MartProductivityByIpZone.business_date.desc())
        else:
            business_date = get_jakarta_shift_business_date()
            query = select(
                func.cast(business_date, Date).label("business_date"),
                MartRealtimeProductivityIpZone.zone_cd,
                MartRealtimeProductivityIpZone.plan_qty,
                MartRealtimeProductivityIpZone.prod_qty,
                MartRealtimeProductivityIpZone.defect_qty,
                MartRealtimeProductivityIpZone.defect_rate,
            )

            if zone_cd:
                # filter by zone code (case insensitive)
                query = query.where(func.upper(MartRealtimeProductivityIpZone.zone_cd) == func.upper(zone_cd))

            query = query.order_by(MartRealtimeProductivityIpZone.zone_cd)

        result = await session.execute(query)
        rows = result.all()

        records = []
        for row in rows:
            records.append(
                ProductivityIpZoneRecord(
                    business_date=row[0],
                    zone_cd=row[1],
                    plan_qty=float(row[2]) if row[2] is not None else None,
                    prod_qty=float(row[3]) if row[3] is not None else None,
                    defect_qty=float(row[4]) if row[4] is not None else None,
                    quality_rate=float(row[5]) if row[5] is not None else None,
                )
            )

        return records

