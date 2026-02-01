"""Service layer for Productivity operations."""

from datetime import date
from typing import List, Optional
from sqlalchemy import select, func
from app.core.database import get_session
from app.models.mart_productivity_by_op_cd import MartProductivityByOpCd
from app.models.mart_productivity_by_op_group import MartProductivityByOpGroup
from app.schemas.productivity import ProductivityRecord


async def fetch_productivity(
    op_cd: Optional[str] = None,
    op_group: Optional[str] = None,
    date: Optional[date] = None,
) -> List[ProductivityRecord]:
    """Productivity data from mart_productivity_by_op_cd or mart_productivity_by_op_group table
    
    Args:
        op_cd: filter by operation code (optional, if not provided, all operation codes)
        op_group: filter by operation group (optional, if not provided, all operation groups)
        date: filter by business date (optional, if not provided, all dates)
    
    Returns:
        Productivity record list
    
    Note:
        - only one of op_cd or op_group must be specified
        - when op_cd is specified: use mart_productivity_by_op_cd table
        - when op_group is specified: use mart_productivity_by_op_group table
    """
    # if both op_cd and op_group are specified or both are not specified, raise an error
    if (op_cd and op_group) or (not op_cd and not op_group):
        raise ValueError("Either 'op_cd' or 'op_group' must be specified (but not both)")
    
    async with get_session(db_alias="montrg") as session:
        if op_cd:
            # use mart_productivity_by_op_cd table (case insensitive)
            query = (
                select(
                    MartProductivityByOpCd.business_date,
                    MartProductivityByOpCd.op_cd,
                    MartProductivityByOpCd.op_group,
                    MartProductivityByOpCd.plan_qty,
                    MartProductivityByOpCd.prod_qty,
                    MartProductivityByOpCd.defect_qty,
                    MartProductivityByOpCd.quality_rate,
                )
                .where(func.upper(MartProductivityByOpCd.op_cd) == func.upper(op_cd))
            )
            
            if date:
                query = query.where(MartProductivityByOpCd.business_date == date)
            
            query = query.order_by(MartProductivityByOpCd.business_date.desc())
            
            result = await session.execute(query)
            rows = result.all()
            
            records = []
            for row in rows:
                records.append(
                    ProductivityRecord(
                        business_date=row[0],
                        op_cd=row[1],
                        op_group=row[2],
                        plan_qty=float(row[3]) if row[3] is not None else None,
                        prod_qty=float(row[4]) if row[4] is not None else None,
                        defect_qty=float(row[5]) if row[5] is not None else None,
                        quality_rate=float(row[6]) if row[6] is not None else None,
                    )
                )
        else:
            # use mart_productivity_by_op_group table (case insensitive)
            query = (
                select(
                    MartProductivityByOpGroup.business_date,
                    MartProductivityByOpGroup.op_group,
                    MartProductivityByOpGroup.plan_qty,
                    MartProductivityByOpGroup.prod_qty,
                    MartProductivityByOpGroup.defect_qty,
                    MartProductivityByOpGroup.quality_rate,
                )
                .where(func.upper(MartProductivityByOpGroup.op_group) == func.upper(op_group))
            )
            
            if date:
                query = query.where(MartProductivityByOpGroup.business_date == date)
            
            query = query.order_by(MartProductivityByOpGroup.business_date.desc())
            
            result = await session.execute(query)
            rows = result.all()
            
            records = []
            for row in rows:
                records.append(
                    ProductivityRecord(
                        business_date=row[0],
                        op_cd=None,
                        op_group=row[1],
                        plan_qty=float(row[2]) if row[2] is not None else None,
                        prod_qty=float(row[3]) if row[3] is not None else None,
                        defect_qty=float(row[4]) if row[4] is not None else None,
                        quality_rate=float(row[5]) if row[5] is not None else None,
                    )
                )
        
        return records

