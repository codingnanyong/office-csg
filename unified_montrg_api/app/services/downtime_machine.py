"""Service layer for Downtime Machine operations."""

from datetime import date
from typing import List, Optional
from sqlalchemy import select, func
from sqlalchemy.orm import aliased
from app.core.database import get_session
from app.models.mart_downtime_by_machine import MartDowntimeByMachine
from app.models.bas_location import BasLocation
from app.schemas.downtime_machine import DowntimeMachineRecord


async def fetch_downtime_machine(
    machine_cd: Optional[str] = None,
    date: Optional[date] = None,
) -> List[DowntimeMachineRecord]:
    """Fetch Production Downtime Machine data (includes bas_location JOIN)
    
    Args:
        machine_cd: filter by Machine code (optional, if not provided, all machines)
        date: filter by business date (optional, if not provided, all dates)
    
    Returns:
        Downtime Machine record list (includes factory_nm, building_nm, line_nm)
    
    Note:
        - silver.mart_downtime_by_machine table (MONTRG_DATABASE_URL is used)
        - JOIN with bronze.bas_location_raw to add name information
    """
    async with get_session(db_alias="montrg") as session:
        # create alias for bas_location table (line, building, factory)
        l = aliased(BasLocation)  # line
        b = aliased(BasLocation)  # building
        f = aliased(BasLocation)  # factory
        
        query = (
            select(
                MartDowntimeByMachine.business_date,
                MartDowntimeByMachine.factory,
                f.loc_nm.label("factory_nm"),
                MartDowntimeByMachine.building,
                b.loc_nm.label("building_nm"),
                MartDowntimeByMachine.line_cd,
                l.loc_nm.label("line_nm"),
                MartDowntimeByMachine.machine_cd,
                MartDowntimeByMachine.mes_machine_nm,
                MartDowntimeByMachine.down_time_target,
                MartDowntimeByMachine.down_time_value,
            )
            .outerjoin(l, l.loc_cd == MartDowntimeByMachine.line_cd)  # line JOIN
            .outerjoin(b, b.loc_cd == l.high2_cd)  # building JOIN
            .outerjoin(f, f.loc_cd == l.high1_cd)  # factory JOIN
        )
        
        if machine_cd:
            # filter by machine code (case insensitive)
            query = query.where(func.upper(MartDowntimeByMachine.machine_cd) == func.upper(machine_cd))
        
        if date:
            query = query.where(MartDowntimeByMachine.business_date == date)
        
        query = query.order_by(MartDowntimeByMachine.business_date.desc())
        
        result = await session.execute(query)
        rows = result.all()
        
        records = []
        for row in rows:
            records.append(
                DowntimeMachineRecord(
                    business_date=row[0],
                    factory=row[1],
                    factory_nm=row[2],
                    building=row[3],
                    building_nm=row[4],
                    line_cd=row[5],
                    line_nm=row[6],
                    machine_cd=row[7],
                    mes_machine_nm=row[8],
                    down_time_target=float(row[9]) if row[9] is not None else None,
                    down_time_value=float(row[10]) if row[10] is not None else None,
                )
            )
        
        return records

