from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from app.database.database import get_db, get_async_db
from app.services.vj.workorder_services import (
    workorder_by_zone,
    workorder_by_zone_async,
    workorder_by_zone_date,
    workorder_by_zone_date_async,
    workorder_by_machine,
    workorder_by_machine_async,
    workorder_by_machine_date,
    workorder_by_machine_date_async
)
from app.schemas.vj.workorder_schmea import WorkOrder

get_vj_db= get_db('vj')
get_vj_async_db = get_async_db('vj')

router = APIRouter()

# Zone

@router.get("/zone",response_model = WorkOrder)
def get_workorder_by_zone(zone:str =None,db: Session = Depends(get_vj_db)):
    workorder_rst = workorder_by_zone(db,zone)
    if not workorder_rst:
        raise HTTPException(status_code=404, detail="No WorkOrder found")
    return workorder_rst

@router.get("/zone/async", response_model = WorkOrder)
async def get_workorder_by_zone_async(zone: str = None, db: AsyncSession = Depends(get_vj_async_db)):
    workorder_rst = await workorder_by_zone_async(db,zone)
    if not workorder_rst:
        raise HTTPException(status_code=404, detail="No WorkOrder found")
    return workorder_rst

@router.get("/zone/date",response_model = WorkOrder)
def get_workorder_by_zone_date(zone:str =None, date: str = None,db: Session = Depends(get_vj_db)):
    workorder_rst = workorder_by_zone_date(db,zone,date)
    if not workorder_rst:
        raise HTTPException(status_code=404, detail="No WorkOrder found")
    return workorder_rst

@router.get("/zone/date/async", response_model = WorkOrder)
async def get_workorder_by_zone_date_async(zone: str = None, date: str = None, db: AsyncSession = Depends(get_vj_async_db)):
    workorder_rst = await workorder_by_zone_date_async(db,zone,date)
    if not workorder_rst:
        raise HTTPException(status_code=404, detail="No WorkOrder found")
    return workorder_rst

# Machine

@router.get("/machine",response_model = WorkOrder)
def get_workorder_by_mach(mach:str =None,db: Session = Depends(get_vj_db)):
    workorder_rst = workorder_by_machine(db,mach)
    if not workorder_rst:
        raise HTTPException(status_code=404, detail="No WorkOrder found")
    return workorder_rst

@router.get("/machine/async", response_model = WorkOrder)
async def get_workorder_by_mach_async(mach: str = None, db: AsyncSession = Depends(get_vj_async_db)):
    workorder_rst = await workorder_by_machine_async(db,mach)
    if not workorder_rst:
        raise HTTPException(status_code=404, detail="No WorkOrder found")
    return workorder_rst

@router.get("/machine/date",response_model = WorkOrder)
def get_workorder_by_mach_date(mach:str =None, date: str = None,db: Session = Depends(get_vj_db)):
    workorder_rst = workorder_by_machine_date(db,mach,date)
    if not workorder_rst:
        raise HTTPException(status_code=404, detail="No WorkOrder found")
    return workorder_rst

@router.get("/machine/date/async", response_model = WorkOrder)
async def get_workorder_by_mach_date_async(mach: str = None, date: str = None, db: AsyncSession = Depends(get_vj_async_db)):
    workorder_rst = await workorder_by_machine_date_async(db,mach,date)
    if not workorder_rst:
        raise HTTPException(status_code=404, detail="No WorkOrder found")
    return workorder_rst