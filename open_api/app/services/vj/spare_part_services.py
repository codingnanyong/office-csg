from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from app.services.helpers.spare_part_helper import get_spare_part,get_spare_part_async
from app.services.helpers.useage_helper import get_usage_data,get_usage_data_async
from app.schemas.vj.zone_schema import Zone
from app.schemas.vj.mach_schema import Mach
from app.schemas.vj.useage_schema import Usage

# Analysis - Zone

def spare_part_by_zone(db: Session, group_by : str = None,zone: str = None, date: str = None) -> Zone:
    result = get_spare_part(db,group_by=group_by,zone=zone,date=date)
    return result

async def spare_part_by_zone_async(db: AsyncSession, group_by : str = None,zone: str = None, date: str = None) -> Zone:
    result = await get_spare_part_async(db,group_by=group_by,zone=zone,date=date)
    return result

# Analysis - Mach

def spare_part_by_mach(db: Session, group_by : str = None,mach: str = None, date: str = None) -> Zone:
    result = get_spare_part(db,group_by=group_by,mach=mach,date=date)
    return result

async def spare_part_by_mach_async(db: AsyncSession, group_by : str = None,mach: str = None, date: str = None) -> Zone:
    result = await get_spare_part_async(db,group_by=group_by,mach=mach,date=date)
    return result

# Usage - Zone

def usage_by_zone(db: Session,zone: str = None,part: str = None,year : str = None) -> Usage:
    result = get_usage_data(db,zone=zone,part=part,year=year)
    return result

async def usage_by_zone_async(db: AsyncSession, zone: str = None,part: str = None,year : str = None) -> Usage:
    result = await get_usage_data_async(db,zone=zone,part=part,year=year)
    return result

# Usage - Mach

def usage_by_mach(db: Session, mach: str = None,part: str = None,year : str = None) -> Usage:
    result = get_usage_data(db,mach=mach,part=part,year=year)
    return result

async def usage_by_mach_async(db: AsyncSession, mach: str = None,part: str = None,year : str = None) -> Usage:
    result = await get_usage_data_async(db,mach=mach,part=part,year=year)
    return result