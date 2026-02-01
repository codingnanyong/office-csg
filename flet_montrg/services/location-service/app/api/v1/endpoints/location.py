"""
Location API endpoints
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from app.core.database import get_db
from app.models.schemas import Location
from app.services.location_service import LocationService
from app.core.exceptions import LocationNotFoundException

router = APIRouter()


@router.get("/", response_model=List[Location])
async def get_all_locations(
    skip: int = Query(0, ge=0, description="건너뛸 레코드 수"),
    limit: int = Query(100, ge=1, le=1000, description="가져올 레코드 수"),
    db: AsyncSession = Depends(get_db)
):
    """모든 센서 위치 정보 조회"""
    service = LocationService(db)
    return await service.get_all_locations(skip=skip, limit=limit)


@router.get("/{sensor_id}", response_model=Location)
async def get_location_by_sensor_id(sensor_id: str, db: AsyncSession = Depends(get_db)):
    """센서 ID로 위치 정보 조회"""
    service = LocationService(db)
    location = await service.get_location_by_sensor_id(sensor_id)
    if not location:
        raise LocationNotFoundException(sensor_id)
    return location


@router.get("/batch/{sensor_ids}")
async def get_locations_by_sensor_ids(sensor_ids: str, db: AsyncSession = Depends(get_db)):
    """여러 센서 ID로 위치 정보 일괄 조회 (쉼표로 구분)"""
    service = LocationService(db)
    sensor_id_list = [s.strip() for s in sensor_ids.split(",")]
    return await service.get_locations_by_sensor_ids(sensor_id_list)


@router.get("/loc/{loc_id}", response_model=Location)
async def get_location_by_loc_id(loc_id: str, db: AsyncSession = Depends(get_db)):
    """위치 ID로 위치 정보 조회"""
    service = LocationService(db)
    location = await service.get_location_by_loc_id(loc_id)
    if not location:
        raise LocationNotFoundException(f"Location with loc_id '{loc_id}' not found")
    return location


@router.get("/loc/batch/{loc_ids}")
async def get_locations_by_loc_ids(loc_ids: str, db: AsyncSession = Depends(get_db)):
    """여러 위치 ID로 위치 정보 일괄 조회 (쉼표로 구분)"""
    service = LocationService(db)
    loc_id_list = [l.strip() for l in loc_ids.split(",")]
    return await service.get_locations_by_loc_ids(loc_id_list)
