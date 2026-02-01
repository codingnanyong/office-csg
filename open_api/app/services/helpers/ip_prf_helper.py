from typing import Union, List, Optional, Tuple, Dict
from sqlalchemy.future import select
from sqlalchemy import and_, func
from sqlalchemy.engine import Row
from app.models.vj.ip_rst_model import IpPrfSchema
from app.schemas.vj.ip_schema import IPModel, IpPrf


def filter(zone: Optional[str] = None, mach: Optional[str] = None, date: Optional[str] = None):
    conditions = []
    if zone:
        conditions.append(IpPrfSchema.zone == zone)
    if mach:
        conditions.append(IpPrfSchema.mach_id == mach)
    if date:
        conditions.append(func.substr(IpPrfSchema.ymd, 1, 4) == date)
    return conditions


def get_unit(zone: Optional[str], mach: Optional[str]) -> Tuple:
    if zone:
        return IpPrfSchema.zone.label("unit_id"), IpPrfSchema.zone
    elif mach:
        return IpPrfSchema.mach_id.label("unit_id"), IpPrfSchema.mach_id
    else:
        return IpPrfSchema.mach_id.label("unit_id"), IpPrfSchema.mach_id


def set_ip_prf(rows: List[Row]) -> Union[IPModel, List[IPModel]]:
    data_dict: Dict[str, List[IpPrf]] = {}
    for row in rows:
        unit_id = row._mapping.get("unit_id")
        date = row._mapping.get("date")
        qty = row._mapping.get("qty")

        ip_data = IpPrf(
            date=date,
            qty=float(qty) if qty is not None else 0.0
        )

        if unit_id in data_dict:
            data_dict[unit_id].append(ip_data)
        else:
            data_dict[unit_id] = [ip_data]

    ip_list = [IPModel(unit_id=key, performance=value) for key, value in data_dict.items()]
    if len(ip_list) == 1:
        return ip_list[0].dict()  
    return [ip.dict() for ip in ip_list] 


def get_ip_rst_data(
        db,
        zone: Optional[str] = None,
        mach: Optional[str] = None,
        date: Optional[str] = None,
    ) -> Union[IPModel, List[IPModel]]:

    unit_column, order_by_column = get_unit(zone, mach)
    date_column = func.substr(IpPrfSchema.ymd, 1, 4).label("date")

    stmt = select(
        unit_column,
        date_column,
        func.sum(IpPrfSchema.total_qty).label("qty")
    )

    conditions = filter(zone, mach, date)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.group_by(unit_column, date_column)
    stmt = stmt.order_by(order_by_column, date_column)

    result = db.execute(stmt)
    rows = result.fetchall()

    return set_ip_prf(rows)


async def get_ip_rst_data_async(
        db,
        zone: Optional[str] = None,
        mach: Optional[str] = None,
        date: Optional[str] = None,
    ) -> Union[IPModel, List[IPModel]]:

    unit_column, order_by_column = get_unit(zone, mach)
    date_column = func.substr(IpPrfSchema.ymd, 1, 4).label("date")

    stmt = select(
        unit_column,
        date_column,
        func.sum(IpPrfSchema.total_qty).label("qty")
    )

    conditions = filter(zone, mach, date)
    if conditions:
        stmt = stmt.where(and_(*conditions))

    stmt = stmt.group_by(unit_column, date_column)
    stmt = stmt.order_by(order_by_column, date_column)

    result = await db.execute(stmt)
    rows = result.fetchall()

    return set_ip_prf(rows)