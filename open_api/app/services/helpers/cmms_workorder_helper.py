from typing import Union, List, Optional, Tuple, Dict
from sqlalchemy.future import select
from sqlalchemy.engine import Row
from sqlalchemy import and_, func
from app.models.vj.cmms_wo_model import WorkOrderSchema
from app.schemas.vj.workorder_schmea import WorkOrder, WorkOrderDetail
from datetime import datetime, timedelta

workorder_columns=[
    WorkOrderSchema.wo_type,
    WorkOrderSchema.wo_yymm,
    WorkOrderSchema.wo_orgn,
    WorkOrderSchema.wo_no,
    WorkOrderSchema.wo_class,
    WorkOrderSchema.work_type,
    WorkOrderSchema.wo_status,
    WorkOrderSchema.request_date,
    WorkOrderSchema.wo_date,
    WorkOrderSchema.problem_date,
    WorkOrderSchema.defe_date.label('defect_date'),
    WorkOrderSchema.solu_date.label('soultion_date'),
    WorkOrderSchema.defe_nm_en.label('defect')
]

def filter(zone: Optional[str] = None, mach: Optional[str] = None, date: Optional[str] = None) -> List[WorkOrder]:
    conditions = []
    if zone:
        conditions.append(WorkOrderSchema.zone == zone)
    if mach:
        conditions.append(WorkOrderSchema.mach_id == mach)
    if date:
        if len(date) == 4:
            conditions.append(func.substr(WorkOrderSchema.wo_yymm, 1, 4) == date)
        elif len(date) == 6:
            conditions.append(WorkOrderSchema.wo_yymm == date)
    else:
        now = datetime.now()
        yyyymm_list = []
        for i in range(3):
            dt = now - timedelta(days=now.day - 1) - timedelta(days=30 * i)
            yyyymm = dt.strftime('%Y%m')
            yyyymm_list.append(yyyymm)
        conditions.append(WorkOrderSchema.wo_yymm.in_(yyyymm_list))
    return conditions

def get_unit(zone: Optional[str], mach: Optional[str]) -> Tuple:
    if zone:
        return WorkOrderSchema.zone.label("unit_id")
    elif mach:
        return WorkOrderSchema.mach_id.label("unit_id")
    else:
        return WorkOrderSchema.mach_id.label("unit_id")
    
def set_workorder(rows: List[Row]) -> Union[WorkOrder, List[WorkOrder]]:
    data_dict: Dict[str, List[WorkOrderDetail]] = {}
    for row in rows:
        unit_id = row._mapping.get("unit_id")

        workorder_data = WorkOrderDetail(
            wo_type=row.wo_type,
            wo_yymm=row.wo_yymm,
            wo_orgn=row.wo_orgn,
            wo_no=row.wo_no,
            wo_class=row.wo_class,
            work_type=row.work_type,
            wo_status=row.wo_status,
            request_date=row.request_date,
            wo_date=row.wo_date,
            problem_date=row.problem_date,
            defect_date=row.defect_date,
            soultion_date=row.soultion_date,
            defect=row.defect
        )

        if unit_id in data_dict:
            data_dict[unit_id].append(workorder_data)
        else:
            data_dict[unit_id] = [workorder_data]

    workorder_list = [WorkOrder(unit_id=key, detail=value) for key, value in data_dict.items()]
    if len(workorder_list) == 1:
        return workorder_list[0].dict()
    return [wo.dict() for wo in workorder_list]

def build_query(unit_column, conditions):
    base_columns = [unit_column] + workorder_columns
    query = select(*base_columns)
    if conditions:
        query = query.where(and_(*conditions))
    return query

def get_workorder_data(
        db,
        zone: Optional[str] = None,
        mach: Optional[str] = None,
        date: Optional[str] = None) -> Union[WorkOrder, List[WorkOrder]]:

    unit_column = get_unit(zone, mach)
    conditions = filter(zone, mach, date)
    query = build_query(unit_column, conditions)
    rows = db.execute(query).fetchall()
    return set_workorder(rows)


async def get_workorder_async_data(
        db,
        zone: Optional[str] = None,
        mach: Optional[str] = None,
        date: Optional[str] = None) -> Union[WorkOrder, List[WorkOrder]]:

    unit_column = get_unit(zone, mach)
    conditions = filter(zone, mach, date)
    query = build_query(unit_column, conditions)
    result = await db.execute(query)
    rows = result.fetchall()
    return set_workorder(rows)