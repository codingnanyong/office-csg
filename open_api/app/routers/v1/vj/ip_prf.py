from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from app.database.database import get_db, get_async_db
from app.services.vj.ip_services import (
    ip_prf_by_zone,
    ip_prf_by_zone_async,
    ip_prf_by_zone_year,
    ip_prf_by_zone_year_async,
    ip_prf_by_machine,
    ip_prf_by_machine_async,
    ip_prf_by_machine_year,
    ip_prf_by_machine_year_async
)
from app.schemas.vj.ip_schema import IPModel

get_vj_db= get_db('vj')
get_vj_async_db = get_async_db('vj')

router = APIRouter()

# Zone 

@router.get("/zone",response_model = IPModel)
def get_ip_performance_by_zone(zone:str =None,db: Session = Depends(get_vj_db)):
    ip_rst = ip_prf_by_zone(db,zone)
    if not ip_rst:
        raise HTTPException(status_code=404, detail="No IP Performance found")
    return ip_rst

@router.get("/zone/async", response_model = IPModel)
async def get_ip_performance_by_zone_async(zone: str = None, db: AsyncSession = Depends(get_vj_async_db)):
    ip_rst = await ip_prf_by_zone_async(db,zone)
    if not ip_rst:
        raise HTTPException(status_code=404, detail="No IP Performance found")
    return ip_rst

@router.get("/zone/year",response_model = IPModel)
def get_ip_performance_by_zone_year(zone:str =None, date: str = None,db: Session = Depends(get_vj_db)):
    ip_rst = ip_prf_by_zone_year(db,zone,date)
    if not ip_rst:
        raise HTTPException(status_code=404, detail="No IP Performance found")
    return ip_rst

@router.get("/zone/year/async", response_model = IPModel)
async def get_ip_performance_by_zone_year_async(zone: str = None, date: str = None,db: AsyncSession = Depends(get_vj_async_db)):
    ip_rst = await ip_prf_by_zone_year_async(db,zone,date)
    if not ip_rst:
        raise HTTPException(status_code=404, detail="No IP Performance found")
    return ip_rst

# Machine

@router.get("/machine",response_model = IPModel)
def get_ip_performance_by_machine(mach:str =None,db: Session = Depends(get_vj_db)):
    ip_rst = ip_prf_by_machine(db,mach)
    if not ip_rst:
        raise HTTPException(status_code=404, detail="No IP Performance found")
    return ip_rst

@router.get("/machine/async", response_model = IPModel)
async def get_ip_performance_by_machine_async(mach: str = None, db: AsyncSession = Depends(get_vj_async_db)):
    ip_rst = await ip_prf_by_machine_async(db,mach)
    if not ip_rst:
        raise HTTPException(status_code=404, detail="No IP Performance found")
    return ip_rst

@router.get("/machine/year",response_model = IPModel)
def get_ip_performance_by_machine_year(mach:str =None, date: str = None,db: Session = Depends(get_vj_db)):
    ip_rst = ip_prf_by_machine_year(db,mach,date)
    if not ip_rst:
        raise HTTPException(status_code=404, detail="No IP Performance found")
    return ip_rst

@router.get("/machine/year/async", response_model = IPModel)
async def get_ip_performance_by_machine_year_async(mach: str = None, date: str = None,db: AsyncSession = Depends(get_vj_async_db)):
    ip_rst = await ip_prf_by_machine_year_async(db,mach,date)
    if not ip_rst:
        raise HTTPException(status_code=404, detail="No IP Performance found")
    return ip_rst
