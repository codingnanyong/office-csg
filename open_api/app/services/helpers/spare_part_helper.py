from sqlalchemy.future import select
from sqlalchemy import func
from app.models.vj.analysis_zone_year_model import ZoneYearSchema
from app.models.vj.analysis_zone_month_model import ZoneMonthSchema
from app.models.vj.analysis_mach_year_model import MachYearSchema
from app.models.vj.analysis_mach_month_model import MachMonthSchema
from app.schemas.vj.zone_schema import Zone
from app.schemas.vj.mach_schema import Mach
from app.schemas.vj.spare_part_schema import SparePart
from collections import defaultdict

def get_model(group_by: str, p_date: str):
    if group_by == "zone_machine":
        return MachYearSchema if len(p_date) == 4 else MachMonthSchema
    else:
        return ZoneYearSchema if len(p_date) == 4 else ZoneMonthSchema


def build_filters(model, group_by: str, zone: str = None, mach: str = None,date :str = None):
    filters = []
    if zone:
        filters.append(func.lower(func.trim(model.zone)) == zone.strip().lower())
    if group_by == "zone_machine" and mach:
        filters.append(func.trim(model.mach_id) == mach.strip())
    if date:
        filters.append(model.date == date)
    return filters

def set_spare_part(results: list, group_by: str = None, p_date: str = None):
    grouped = defaultdict(list)

    for row in results:
        spare_part = SparePart(
            part_cd=row.part_cd,
            part_nm=row.part_nm_en,
            cycle_dt=row.cycle_dt,
            current_wo_dt=row.current_wo_date,
            previous_wo_dt=row.previous_wo_date,
            total_qty=row.total_qty,
            previous_total_qty=row.total_qty_recent,
            min=row.min_qty,
            max=row.max_qty,
            stock_qty=row.stock_qty,
            rn=row.rn,
        )
        if group_by == "zone_machine":
            grouped[(row.zone, row.mach_id)].append(spare_part)
        else:
            grouped[row.zone].append(spare_part)

    if group_by == "zone_machine":
        for (zone, mach_id), parts in grouped.items():
            return Mach(date=p_date, zone=zone, mach_id=mach_id, parts=parts)
    else:
        for zone, parts in grouped.items():
            return Zone(date=p_date, zone=zone, parts=parts)

    return None


def get_spare_part(db, group_by: str = None, zone: str = None, mach: str = None, date: str = None):
    model = get_model(group_by, date)
    filters = build_filters(model, group_by, zone, mach, date)
    stmt = select(model).where(*filters) if filters else select(model)
    results = db.execute(stmt).scalars().all()
    return set_spare_part(results, group_by, date)


async def get_spare_part_async(db, group_by: str = None, zone: str = None, mach: str = None, date: str = None):
    model = get_model(group_by, date)
    filters = build_filters(model, group_by, zone, mach, date)
    stmt = select(model).where(*filters) if filters else select(model)
    results = (await db.execute(stmt)).scalars().all()
    return set_spare_part(results, group_by, date)
