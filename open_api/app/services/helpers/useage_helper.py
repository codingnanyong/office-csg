from typing import Optional, Tuple, Dict, Union, List
from sqlalchemy.future import select
from sqlalchemy import and_,func
from sqlalchemy.engine import Row
from app.models.vj.spare_part_use_model import SparePartUseSchema
from app.schemas.vj.useage_schema import Usage,SparePartUse

def filter(
        zone: Optional[str] = None, 
        mach: Optional[str] = None,
        part: str = None,
        year: Optional[str] = None
    ):
    conditions = []
    if zone:
        conditions.append(SparePartUseSchema.zone == zone)
    if mach:
        conditions.append(SparePartUseSchema.mach_id == mach)
    if part:
        conditions.append(SparePartUseSchema.part_cd == part)
    if year:
        conditions.append(SparePartUseSchema.year == year)
    return conditions

def get_unit(zone: Optional[str], mach: Optional[str]) -> Tuple:
    if zone:
        return SparePartUseSchema.zone.label("unit_id"), SparePartUseSchema.zone
    elif mach:
        return SparePartUseSchema.mach_id.label("unit_id"), SparePartUseSchema.mach_id
    else:
        return SparePartUseSchema.mach_id.label("unit_id"), SparePartUseSchema.mach_id
    

def set_usage(rows: List[Row]) -> Union[Usage, List[Usage]]:
    data_dict: Dict[Tuple[str, str], List[SparePartUse]] = {}

    for row in rows:
        row_dict = row._mapping
        unit_id = row_dict.get("unit_id")
        part_cd = row_dict.get("part_cd")

        usage_item = SparePartUse(
            year=row_dict.get("year"),
            month=row_dict.get("month"),
            useage=row_dict.get("monthly_usage") or 0.0
        )

        key = (unit_id, part_cd)
        data_dict.setdefault(key, []).append(usage_item)

    result: List[Usage] = [Usage(unit_id=k[0], partcd=k[1], useage=v) for k, v in data_dict.items()]

    if len(result) == 1:
        return result[0].dict()
    return [r.dict() for r in result]

def get_usage_data(
    db,
    zone: Optional[str] = None,
    mach: Optional[str] = None,
    part: str = None,
    year: Optional[str] = None
) -> Union[Usage, list[Usage]]:
    unit_column, order_by_column = get_unit(zone, mach)
    stmt = select(
        unit_column,
        SparePartUseSchema.part_cd,
        SparePartUseSchema.year,
        SparePartUseSchema.month,
        func.sum(SparePartUseSchema.monthly_usage).label("monthly_usage")
    )

    conditions = filter(zone, mach,part,year)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.group_by(unit_column, SparePartUseSchema.part_cd, SparePartUseSchema.year, SparePartUseSchema.month)
    stmt = stmt.order_by(order_by_column, SparePartUseSchema.part_cd, SparePartUseSchema.year, SparePartUseSchema.month)

    results = db.execute(stmt).all()
    return set_usage(results)

async def get_usage_data_async(
    db,
    zone: Optional[str] = None,
    mach: Optional[str] = None,
    part: str = None,
    year: Optional[str] = None
) -> Union[Usage, list[Usage]]:
    unit_column, order_by_column = get_unit(zone, mach)
    stmt = select(
        unit_column,
        SparePartUseSchema.part_cd,
        SparePartUseSchema.year,
        SparePartUseSchema.month,
        func.sum(SparePartUseSchema.monthly_usage).label("monthly_usage")
    )
    conditions = filter(zone, mach,part,year)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.group_by(unit_column, SparePartUseSchema.part_cd, SparePartUseSchema.year, SparePartUseSchema.month)
    stmt = stmt.order_by(order_by_column, SparePartUseSchema.part_cd, SparePartUseSchema.year, SparePartUseSchema.month)

    result = await db.execute(stmt)
    rows = result.fetchall()
    return set_usage(rows)