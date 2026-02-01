"""Service layer for Productivity PH Machine operations."""

from datetime import date
from typing import List, Optional
from sqlalchemy import Date, cast, func, select
from app.core.database import get_session
from app.models.mart_productivity_by_ph_machine import MartProductivityByPhMachine
from app.models.mart_realtime_productivity_ph_machine import MartRealtimeProductivityPhMachine
from app.schemas.productivity_ph_machine import ProductivityPhMachineRecord
from app.services.productivity_shift import get_jakarta_shift_business_date


async def fetch_productivity_ph_machine(
    machine_cd: Optional[str] = None,
    date: Optional[date] = None,
) -> List[ProductivityPhMachineRecord]:
    """Productivity PH Machine data from mart_productivity_by_ph_machine table
    
    Args:
        machine_cd: filter by machine code (optional, if not provided, all machines)
        date: filter by business date (optional, if not provided, all dates)
    
    Returns:
        Productivity PH Machine record list
    
    Note:
        - silver.mart_productivity_by_ph_machine table
    """
    async with get_session(db_alias="montrg") as session:
        if date:
            query = select(
                MartProductivityByPhMachine.business_date,
                MartProductivityByPhMachine.machine_cd,
                MartProductivityByPhMachine.plan_qty,
                MartProductivityByPhMachine.prod_qty,
                MartProductivityByPhMachine.defect_qty,
                MartProductivityByPhMachine.quality_rate,
            )

            if machine_cd:
                # filter by machine code (case insensitive)
                query = query.where(func.upper(MartProductivityByPhMachine.machine_cd) == func.upper(machine_cd))

            query = query.where(MartProductivityByPhMachine.business_date == date)
            query = query.order_by(MartProductivityByPhMachine.business_date.desc())
        else:
            business_date = get_jakarta_shift_business_date()
            query = select(
                func.cast(business_date, Date).label("business_date"),
                MartRealtimeProductivityPhMachine.machine_cd,
                MartRealtimeProductivityPhMachine.plan_qty,
                MartRealtimeProductivityPhMachine.prod_qty,
                MartRealtimeProductivityPhMachine.defect_qty,
                MartRealtimeProductivityPhMachine.defect_rate,
            )

            if machine_cd:
                # filter by machine code (case insensitive)
                query = query.where(func.upper(MartRealtimeProductivityPhMachine.machine_cd) == func.upper(machine_cd))

            query = query.order_by(MartRealtimeProductivityPhMachine.machine_cd)

        result = await session.execute(query)
        rows = result.all()

        records = []
        for row in rows:
            records.append(
                ProductivityPhMachineRecord(
                    business_date=row[0],
                    machine_cd=row[1],
                    plan_qty=float(row[2]) if row[2] is not None else None,
                    prod_qty=float(row[3]) if row[3] is not None else None,
                    defect_qty=float(row[4]) if row[4] is not None else None,
                    quality_rate=float(row[5]) if row[5] is not None else None,
                )
            )

        return records

