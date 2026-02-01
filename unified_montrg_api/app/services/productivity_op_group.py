"""Service layer for Productivity OP Group operations."""

from datetime import date
from typing import List, Optional
from sqlalchemy import Date, cast, func, select
from app.core.database import get_session
from app.models.mart_productivity_by_op_group import MartProductivityByOpGroup
from app.models.mart_realtime_productivity_op_group import MartRealtimeProductivityOpGroup
from app.schemas.productivity_op_group import ProductivityOpGroupRecord
from app.services.productivity_shift import get_jakarta_shift_business_date


async def fetch_productivity_op_group(
    op_group: Optional[str] = None,
    date: Optional[date] = None,
) -> List[ProductivityOpGroupRecord]:
    """Fetch Productivity OP Group data from mart_productivity_by_op_group table    
    
    Args:
        op_group: filter by operation group (optional, if not provided, all operation groups)
        date: filter by business date (optional, if not provided, all dates)
    
    Returns:
        Productivity OP Group record list
    
    Note:
        - silver.mart_productivity_by_op_group table
    """
    async with get_session(db_alias="montrg") as session:
        if date:
            query = select(
                MartProductivityByOpGroup.business_date,
                MartProductivityByOpGroup.op_group,
                MartProductivityByOpGroup.plan_qty,
                MartProductivityByOpGroup.prod_qty,
                MartProductivityByOpGroup.defect_qty,
                MartProductivityByOpGroup.quality_rate,
            )

            if op_group:
                # filter by operation group (case insensitive)
                query = query.where(func.upper(MartProductivityByOpGroup.op_group) == func.upper(op_group))

            query = query.where(MartProductivityByOpGroup.business_date == date)
            query = query.order_by(MartProductivityByOpGroup.business_date.desc())
        else:
            business_date = get_jakarta_shift_business_date()
            query = select(
                func.cast(business_date, Date).label("business_date"),
                MartRealtimeProductivityOpGroup.op_group,
                MartRealtimeProductivityOpGroup.plan_qty,
                MartRealtimeProductivityOpGroup.prod_qty,
                MartRealtimeProductivityOpGroup.defect_qty,
                MartRealtimeProductivityOpGroup.defect_rate,
            )

            if op_group:
                # filter by operation group (case insensitive)
                query = query.where(func.upper(MartRealtimeProductivityOpGroup.op_group) == func.upper(op_group))

            query = query.order_by(MartRealtimeProductivityOpGroup.op_group)

        result = await session.execute(query)
        rows = result.all()

        records = []
        for row in rows:
            records.append(
                ProductivityOpGroupRecord(
                    business_date=row[0],
                    op_group=row[1],
                    plan_qty=float(row[2]) if row[2] is not None else None,
                    prod_qty=float(row[3]) if row[3] is not None else None,
                    defect_qty=float(row[4]) if row[4] is not None else None,
                    quality_rate=float(row[5]) if row[5] is not None else None,
                )
            )

        return records

