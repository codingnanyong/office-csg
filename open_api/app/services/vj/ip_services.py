from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.helpers.ip_prf_helper import get_ip_rst_data,get_ip_rst_data_async
from app.schemas.vj.ip_schema import IPModel

# Zone

def ip_prf_by_zone(db: Session, zone: str = None) -> IPModel:
    result = get_ip_rst_data(db, zone = zone)
    return result

async def ip_prf_by_zone_async (db: AsyncSession, zone: str = None) -> IPModel:
    result = await get_ip_rst_data_async(db,zone=zone)
    return result

def ip_prf_by_zone_year(db: Session, zone: str = None, date: str = None) -> IPModel:
    result = get_ip_rst_data(db, zone = zone,date=date)
    return result

async def ip_prf_by_zone_year_async (db: AsyncSession, zone: str = None, date: str = None) -> IPModel:
    result = await get_ip_rst_data_async(db,zone=zone,date=date)
    return result

# Machine

def ip_prf_by_machine(db: Session, mach: str = None) -> IPModel:
    result = get_ip_rst_data(db, mach = mach)
    return result

async def ip_prf_by_machine_async (db: AsyncSession, mach: str = None) -> IPModel:
    result = await get_ip_rst_data_async(db,mach=mach)
    return result

def ip_prf_by_machine_year(db: Session, mach: str = None, date: str = None) -> IPModel:
    result = get_ip_rst_data(db, mach = mach,date=date)
    return result

async def ip_prf_by_machine_year_async (db: AsyncSession, mach: str = None, date: str = None) -> IPModel:
    result = await get_ip_rst_data_async(db,mach=mach,date=date)
    return result