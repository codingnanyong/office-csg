from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from app.database.database import get_db, get_async_db
from app.services.hq.iot_services import (
    latest_temperature,
    latest_temperature_async,
    today_temperature,
    today_temperature_async,
    range_temperature,
    range_temperature_async
)
from app.schemas.hq.iot_schema import IoT

get_hq_db= get_db('hq')
get_hq_async_db = get_async_db('hq')

router = APIRouter()

@router.get("/temperature",response_model=IoT)
def get_latest_temperature(loc:str , db : Session=Depends(get_hq_db)):
    iot = latest_temperature(db,loc)
    if not iot:
        raise HTTPException(status_code=404, detail="No IoT found")
    return iot

@router.get("/temperature/async",response_model=IoT)
async def get_latest_temperature_async(loc:str , db : AsyncSession=Depends(get_hq_async_db)):
    iot = await latest_temperature_async(db,loc)
    if not iot:
        raise HTTPException(status_code=404, detail="No IoT found")
    return iot

@router.get("/temperature/today",response_model=IoT)
def get_today_temperature(loc:str , db : Session=Depends(get_hq_db)):
    iot = today_temperature(db,loc)
    if not iot:
        raise HTTPException(status_code=404, detail="No IoT found")
    return iot

@router.get("/temperature/today/async",response_model=IoT)
async def get_today_temperature_async(loc:str , db : AsyncSession=Depends(get_hq_async_db)):
    iot = await today_temperature_async(db,loc)
    if not iot:
        raise HTTPException(status_code=404, detail="No IoT found")
    return iot

@router.get("/temperature/range",response_model=IoT)
def get_range_temperature(loc:str , start_date:str, end_date: str, db : Session=Depends(get_hq_db)):
    try:
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")
    except ValueError:
        raise ValueError("Invalid date format. Use yyyyMMdd")
    
    if start_dt > end_dt:
        raise ValueError("start_date must be earlier than end_date")
    
    iot = range_temperature(db,loc,start_date=start_date,end_date=end_date)

    if not iot:
        raise HTTPException(status_code=404, detail="No IoT found")
    
    return iot

@router.get("/temperature/range/async",response_model=IoT)
async def get_range_temperature_async(loc:str , start_date:str, end_date: str, db : AsyncSession=Depends(get_hq_async_db)):
    try:
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")
    except ValueError:
        raise ValueError("Invalid date format. Use yyyyMMdd")
    
    if start_dt > end_dt:
        raise ValueError("start_date must be earlier than end_date")
    
    iot = await range_temperature_async(db,loc,start_date=start_date,end_date=end_date)

    if not iot:
        raise HTTPException(status_code=404, detail="No IoT found")
    
    return iot