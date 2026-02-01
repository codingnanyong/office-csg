"""Service layer for Downtime Line operations."""

from datetime import date
from typing import List, Optional
from sqlalchemy import select, func
from sqlalchemy.orm import aliased
from app.core.database import get_session
from app.models.mart_downtime_by_line import MartDowntimeByLine
from app.models.bas_location import BasLocation
from app.schemas.downtime_line import DowntimeLineRecord


async def fetch_downtime_line(
    line_cd: Optional[str] = None,
    date: Optional[date] = None,
) -> List[DowntimeLineRecord]:
    """Fetch Production Downtime Line data (includes bas_location JOIN)
    
    Args:
        line_cd: filter by Line code (optional, if not provided, all lines)
        date: filter by business date (optional, if not provided, all dates)
    
    Returns:
        Downtime Line record list (includes factory_nm, building_nm, line_nm)
    
    Note:
        - silver.mart_downtime_by_line table (MONTRG_DATABASE_URL is used)
        - JOIN with bronze.bas_location_raw to add name information
    """
    async with get_session(db_alias="montrg") as session:
        # create alias for bas_location table (line, building, factory)
        l = aliased(BasLocation)  # line
        b = aliased(BasLocation)  # building
        f = aliased(BasLocation)  # factory
        
        query = (
            select(
                MartDowntimeByLine.business_date,
                MartDowntimeByLine.factory,
                f.loc_nm.label("factory_nm"),
                MartDowntimeByLine.building,
                b.loc_nm.label("building_nm"),
                MartDowntimeByLine.line_cd,
                l.loc_nm.label("line_nm"),
                MartDowntimeByLine.down_time_target,
                MartDowntimeByLine.down_time_value,
            )
            .outerjoin(l, l.loc_cd == MartDowntimeByLine.line_cd)  # line JOIN
            .outerjoin(b, b.loc_cd == l.high2_cd)  # building JOIN
            .outerjoin(f, f.loc_cd == l.high1_cd)  # factory JOIN
        )
        
        if line_cd:
            # filter by line code (case insensitive)
            query = query.where(func.upper(MartDowntimeByLine.line_cd) == func.upper(line_cd))
        
        if date:
            query = query.where(MartDowntimeByLine.business_date == date)
        
        query = query.order_by(MartDowntimeByLine.business_date.desc())
        
        result = await session.execute(query)
        rows = result.all()
        
        records = []
        for row in rows:
            records.append(
                DowntimeLineRecord(
                    business_date=row[0],
                    factory=row[1],
                    factory_nm=row[2],
                    building=row[3],
                    building_nm=row[4],
                    line_cd=row[5],
                    line_nm=row[6],
                    down_time_target=float(row[7]) if row[7] is not None else None,
                    down_time_value=float(row[8]) if row[8] is not None else None,
                )
            )
        
        return records

