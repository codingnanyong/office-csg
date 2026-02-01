from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from app.services.helpers.cmms_workorder_helper import get_workorder_data, get_workorder_async_data
from app.schemas.vj.workorder_schmea import WorkOrder

# Zone

def workorder_by_zone(db: Session, zone: str = None) -> WorkOrder:
    return get_workorder_data(db, zone=zone)

async def workorder_by_zone_async(db: AsyncSession, zone: str = None) -> WorkOrder:
    return await get_workorder_async_data(db, zone=zone)

def workorder_by_zone_date(db: Session, zone: str = None, date: str = None) -> WorkOrder:
    if date is None or (date and (len(date) == 4 or len(date) == 6)):
        return get_workorder_data(db, zone=zone, date=date)
    else:
        raise HTTPException(status_code=400, detail="Invalid date format. Must be yyyy or yyyyMM or null.")

async def workorder_by_zone_date_async(db: AsyncSession, zone: str = None, date: str = None) -> WorkOrder:
    if date is None or (date and (len(date) == 4 or len(date) == 6)):
        return await get_workorder_async_data(db, zone=zone, date=date)
    else:
        raise HTTPException(status_code=400, detail="Invalid date format. Must be yyyy or yyyyMM or null.")

# Machine

def workorder_by_machine(db: Session, mach: str = None) -> WorkOrder:
    return get_workorder_data(db, mach=mach)

async def workorder_by_machine_async(db: AsyncSession, mach: str = None) -> WorkOrder:
    return await get_workorder_async_data(db, mach=mach)

def workorder_by_machine_date(db: Session, mach: str = None, date: str = None) -> WorkOrder:
    if date is None or (date and (len(date) == 4 or len(date) == 6)):
        return get_workorder_data(db, mach=mach, date=date)
    else:
        raise HTTPException(status_code=400, detail="Invalid date format. Must be yyyy or yyyyMM or null.")

async def workorder_by_machine_date_async(db: AsyncSession, mach: str = None, date: str = None) -> WorkOrder:
    if date is None or (date and (len(date) == 4 or len(date) == 6)):
        return await get_workorder_async_data(db, mach=mach, date=date)
    else:
        raise HTTPException(status_code=400, detail="Invalid date format. Must be yyyy or yyyyMM or null.")