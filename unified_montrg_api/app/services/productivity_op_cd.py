"""Service layer for Productivity OP CD operations."""

from datetime import date
from typing import List, Optional
from sqlalchemy import Date, cast, func, select
from app.core.database import get_session
from app.models.mart_productivity_by_op_cd import MartProductivityByOpCd
from app.models.mart_realtime_productivity_op_cd import MartRealtimeProductivityOpCd
from app.schemas.productivity_op_cd import ProductivityOpCdRecord
from app.services.productivity_shift import get_jakarta_shift_business_date


async def fetch_productivity_op_cd(
    op_cd: Optional[str] = None,
    date: Optional[date] = None,
) -> List[ProductivityOpCdRecord]:
    """Fetch Productivity OP CD data from mart_productivity_by_op_cd table
    
    Args:
        op_cd: filter by operation code (optional, if not provided, all operation codes)
        date: filter by business date (optional, if not provided, all dates)
    
    Returns:
        Productivity OP CD record list
    
    Note:
        - silver.mart_productivity_by_op_cd table
    """
    async with get_session(db_alias="montrg") as session:
        if date:
            query = select(
                MartProductivityByOpCd.business_date,
                MartProductivityByOpCd.op_cd,
                MartProductivityByOpCd.op_group,
                MartProductivityByOpCd.plan_qty,
                MartProductivityByOpCd.prod_qty,
                MartProductivityByOpCd.defect_qty,
                MartProductivityByOpCd.quality_rate,
            )

            if op_cd:
                # filter by operation code (case insensitive)
                query = query.where(func.upper(MartProductivityByOpCd.op_cd) == func.upper(op_cd))

            query = query.where(MartProductivityByOpCd.business_date == date)
            query = query.order_by(MartProductivityByOpCd.business_date.desc())
        else:
            business_date = get_jakarta_shift_business_date()
            query = select(
                func.cast(business_date, Date).label("business_date"),
                MartRealtimeProductivityOpCd.op_cd,
                MartRealtimeProductivityOpCd.op_group,
                MartRealtimeProductivityOpCd.plan_qty,
                MartRealtimeProductivityOpCd.prod_qty,
                MartRealtimeProductivityOpCd.defect_qty,
                MartRealtimeProductivityOpCd.defect_rate,
            )

            if op_cd:
                # filter by operation code (case insensitive)
                query = query.where(func.upper(MartRealtimeProductivityOpCd.op_cd) == func.upper(op_cd))

            query = query.order_by(MartRealtimeProductivityOpCd.op_cd)

        result = await session.execute(query)
        rows = result.all()

        records = []
        for row in rows:
            records.append(
                ProductivityOpCdRecord(
                    business_date=row[0],
                    op_cd=row[1],
                    op_group=row[2],
                    plan_qty=float(row[3]) if row[3] is not None else None,
                    prod_qty=float(row[4]) if row[4] is not None else None,
                    defect_qty=float(row[5]) if row[5] is not None else None,
                    quality_rate=float(row[6]) if row[6] is not None else None,
                )
            )

        return records

