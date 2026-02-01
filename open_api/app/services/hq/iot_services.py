from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.hq.iot_schema import IoT
from app.services.helpers.iot_helper import get_iot_data,get_iot_data_async

def latest_temperature(db: Session, loc: str = None) -> IoT:
    result = get_iot_data(db, loc=loc, latest=True)
    return result  

async def latest_temperature_async(db: AsyncSession, loc: str = None) -> IoT:
    result = await get_iot_data_async(db, loc=loc, latest=True)
    return result  


def today_temperature(db: Session, loc: str = None) -> IoT:
    result = get_iot_data(db, loc=loc, today=True)
    return result  

async def today_temperature_async(db: AsyncSession, loc: str = None) -> IoT:
    result = await get_iot_data_async(db, loc=loc, today=True)
    return result  

def range_temperature(db: Session, loc: str = None, start_date: str = None, end_date: str = None) -> IoT:
    if start_date > end_date:
        raise ValueError("start_date must be earlier than end_date")  

    result = get_iot_data(db, loc=loc, start_date=start_date, end_date=end_date)
    return result 

async def range_temperature_async(db: AsyncSession, loc: str = None, start_date: str = None, end_date: str = None) -> IoT:
    if start_date > end_date:
        raise ValueError("start_date must be earlier than end_date")  

    result = await get_iot_data_async(db, loc=loc, start_date=start_date, end_date=end_date)
    return result  