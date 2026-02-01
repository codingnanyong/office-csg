"""
Realtime API endpoints
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from app.core.database import get_db
from app.models.schemas import TemperatureCurrentData
from app.services.temperature_service import TemperatureService

router = APIRouter()


@router.get("/", response_model=TemperatureCurrentData)
async def get_current_temperature_data(
    db: AsyncSession = Depends(get_db)
):
    """현재 온도 데이터 조회 (임계치 검사 포함)"""
    service = TemperatureService(db)
    data = await service.get_current_temperature_data()
    
    return data


@router.get("/factory/{factory}", response_model=TemperatureCurrentData)
async def get_temperature_data_by_factory(
    factory: str,
    db: AsyncSession = Depends(get_db)
):
    """공장별 온도 데이터 조회"""
    service = TemperatureService(db)
    data = await service.get_current_temperature_data_by_factory(factory)
    
    return data


@router.get("/building/{building}", response_model=TemperatureCurrentData)
async def get_temperature_data_by_building(
    building: str,
    db: AsyncSession = Depends(get_db)
):
    """건물별 온도 데이터 조회"""
    service = TemperatureService(db)
    data = await service.get_current_temperature_data_by_building(building)
    
    return data


@router.get("/floor/{floor}", response_model=TemperatureCurrentData)
async def get_temperature_data_by_floor(
    floor: int,
    db: AsyncSession = Depends(get_db)
):
    """층별 온도 데이터 조회"""
    service = TemperatureService(db)
    data = await service.get_current_temperature_data_by_floor(floor)
    
    return data


@router.get("/loc_id/{loc_id}", response_model=TemperatureCurrentData)
async def get_temperature_data_by_loc_id(
    loc_id: str,
    db: AsyncSession = Depends(get_db)
):
    """위치 ID별 온도 데이터 조회"""
    service = TemperatureService(db)
    data = await service.get_current_temperature_data_by_loc_id(loc_id)
    
    return data


@router.get("/location", response_model=TemperatureCurrentData)
async def get_temperature_data_by_location(
    factory: Optional[str] = Query(None, description="공장명"),
    building: Optional[str] = Query(None, description="건물명"),
    floor: Optional[int] = Query(None, description="층수"),
    loc_id: Optional[str] = Query(None, description="위치 ID"),
    db: AsyncSession = Depends(get_db)
):
    """위치 조건별 온도 데이터 조회 (다중 필터 지원)"""
    service = TemperatureService(db)
    data = await service.get_current_temperature_data_by_location(
        factory=factory, 
        building=building, 
        floor=floor,
        loc_id=loc_id
    )
    
    return data
