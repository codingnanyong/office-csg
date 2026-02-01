from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from app.database.database import get_db, get_async_db
from app.services.vj.spare_part_services import (
    spare_part_by_zone,
    spare_part_by_zone_async,
    spare_part_by_mach,
    spare_part_by_mach_async,
    usage_by_zone,
    usage_by_zone_async,
    usage_by_mach,
    usage_by_mach_async
)
from app.schemas.vj.zone_schema import Zone
from app.schemas.vj.mach_schema import Mach
from app.schemas.vj.useage_schema import Usage

get_vj_db= get_db('vj')
get_vj_async_db = get_async_db('vj')

router = APIRouter()

# Analysis - Zone 

@router.get("/zone",response_model = Zone)
def get_spare_part_by_zone(
    zone: str = Query(None, description="Zone ID"),
    date: str = Query(None, description="Year or Month Ex: yyyy or yyyyMM"),
    db: Session = Depends(get_vj_db)
):
    group = 'zone'
    results = spare_part_by_zone(db=db, group_by=group, zone=zone, date=date)

    if not results:
        raise HTTPException(status_code=404, detail="No Spare Part found")
    return results

@router.get("/zone/async",response_model = Zone)
async def get_spare_part_by_zone_async(
    zone: str = Query(None, description="Zone ID"),
    date: str = Query(None, description="Year or Month Ex: yyyy or yyyyMM"),
    db: AsyncSession = Depends(get_vj_async_db)
):
    group = 'zone'
    results = await spare_part_by_zone_async(db=db, group_by=group, zone=zone, date=date)

    if not results:
        raise HTTPException(status_code=404, detail="No Spare Part found")
    return results

# Analysis - Mach  

@router.get("/mach",response_model = Mach)
def get_spare_part_by_mach(
    mach: str = Query(None, description="Machine ID"),
    date: str = Query(None, description="Year or Month Ex: yyyy or yyyyMM"),
    db: Session = Depends(get_vj_db)
):
    group = 'zone_machine'
    results = spare_part_by_mach(db=db, group_by=group, mach=mach, date=date)

    if not results:
        raise HTTPException(status_code=404, detail="No Spare Part found")
    return results

@router.get("/mach/async",response_model = Mach)
async def get_spare_part_by_mach_async(
    mach: str = Query(None, description="Machine ID"),
    date: str = Query(None, description="Year or Month Ex: yyyy or yyyyMM"),
    db: AsyncSession = Depends(get_vj_async_db)
):
    group = 'zone_machine'
    results = await spare_part_by_mach_async(db=db, group_by=group, mach=mach, date=date)

    if not results:
        raise HTTPException(status_code=404, detail="No Spare Part found")
    return results


# Usage - Zone 

@router.get("/zone/usage",response_model = Usage)
def get_usage_by_zone(
    zone: str = Query(None, description="Zone ID"),
    part: str = Query(None, description="Part Code"),
    year: int = Query(None, description="Date Foramat yyyy"),
    db: Session = Depends(get_vj_db)
):
    results = usage_by_zone(db=db, zone=zone, part=part,year=year)

    if not results:
        raise HTTPException(status_code=404, detail="No Usage found")
    return results

@router.get("/zone/usage/async",response_model = Usage)
async def get_usage_by_zone_async(
    zone: str = Query(None, description="Zone ID"),
    part: str = Query(None, description="Part Code"),
    year: int = Query(None, description="Date Foramat yyyy"),
    db: AsyncSession = Depends(get_vj_async_db)
):
    group = 'zone'
    results = await usage_by_zone_async(db=db, zone=zone, part=part,year=year)

    if not results:
        raise HTTPException(status_code=404, detail="No Usage found")
    return results

# Usage - Mach  

@router.get("/mach/usage",response_model = Usage)
def get_usage_by_mach(
    mach: str = Query(None, description="Machine ID"),
    part: str = Query(None, description="Part Code"),
    year: int = Query(None, description="Date Foramat yyyy"),
    db: Session = Depends(get_vj_db)
):
    results = usage_by_mach(db=db, mach=mach, part=part,year=year)

    if not results:
        raise HTTPException(status_code=404, detail="No Usage found")
    return results

@router.get("/mach/usage/async",response_model = Usage)
async def get_usage_by_mach_async(
    mach: str = Query(None, description="Machine ID"),
    part: str = Query(None, description="Part Code"),
    year: int = Query(None, description="Date Foramat yyyy"),
    db: AsyncSession = Depends(get_vj_async_db)
):
    results = await usage_by_mach_async(db=db, mach=mach, part=part,year=year)

    if not results:
        raise HTTPException(status_code=404, detail="No Usage found")
    return results