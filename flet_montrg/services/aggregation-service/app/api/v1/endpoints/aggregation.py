"""
Aggregation API endpoints
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional

from app.core.database import get_db
from app.models.schemas import (
    AggregationRequest, 
    TemperatureAggregationResponse
)
from app.services.aggregation_service import AggregationService
from app.core.exceptions import DataNotFoundError

router = APIRouter()


@router.get("/pcv_temperature/", response_model=TemperatureAggregationResponse)
async def get_all_pcv_temperature(
    start_date: str = Query(..., description="시작 날짜 (yyyy, yyyyMM, yyyyMMdd)"),
    end_date: str = Query(..., description="종료 날짜 (yyyy, yyyyMM, yyyyMMdd)"),
    db: AsyncSession = Depends(get_db)
):
    """모든 체감 온도(max,avg)의 집계 데이터 조회"""
    service = AggregationService(db)
    request = AggregationRequest(
        start_date=start_date,
        end_date=end_date
    )
    try:
        return await service.get_temperature_aggregation(request)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/pcv_temperature/location/{location_id}", response_model=TemperatureAggregationResponse)
async def get_pcv_temperature_by_location(
    location_id: str,
    start_date: str = Query(..., description="시작 날짜 (yyyy, yyyyMM, yyyyMMdd)"),
    end_date: str = Query(..., description="종료 날짜 (yyyy, yyyyMM, yyyyMMdd)"),
    db: AsyncSession = Depends(get_db)
):
    """위치별 체감 온도 조회"""
    service = AggregationService(db)
    request = AggregationRequest(
        location_id=location_id,
        start_date=start_date,
        end_date=end_date
    )
    try:
        return await service.get_temperature_aggregation(request)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/pcv_temperature/factory/{factory}", response_model=TemperatureAggregationResponse)
async def get_pcv_temperature_by_factory(
    factory: str,
    start_date: str = Query(..., description="시작 날짜 (yyyy, yyyyMM, yyyyMMdd)"),
    end_date: str = Query(..., description="종료 날짜 (yyyy, yyyyMM, yyyyMMdd)"),
    db: AsyncSession = Depends(get_db)
):
    """공장별 체감 온도 집계 조회 (공장 전체의 MAX, AVG)"""
    service = AggregationService(db)
    request = AggregationRequest(
        factory=factory,
        start_date=start_date,
        end_date=end_date
    )
    try:
        return await service.get_temperature_aggregation(request)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/pcv_temperature/building/{factory}/{building}", response_model=TemperatureAggregationResponse)
async def get_pcv_temperature_by_building(
    factory: str,
    building: str,
    start_date: str = Query(..., description="시작 날짜 (yyyy, yyyyMM, yyyyMMdd)"),
    end_date: str = Query(..., description="종료 날짜 (yyyy, yyyyMM, yyyyMMdd)"),
    db: AsyncSession = Depends(get_db)
):
    """건물별 체감 온도 집계 조회 (건물 전체의 MAX, AVG)"""
    service = AggregationService(db)
    request = AggregationRequest(
        factory=factory,
        building=building,
        start_date=start_date,
        end_date=end_date
    )
    try:
        return await service.get_temperature_aggregation(request)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/pcv_temperature/floor/{factory}/{building}/{floor}", response_model=TemperatureAggregationResponse)
async def get_pcv_temperature_by_floor(
    factory: str,
    building: str,
    floor: int,
    start_date: str = Query(..., description="시작 날짜 (yyyy, yyyyMM, yyyyMMdd)"),
    end_date: str = Query(..., description="종료 날짜 (yyyy, yyyyMM, yyyyMMdd)"),
    db: AsyncSession = Depends(get_db)
):
    """층별 체감 온도 집계 조회 (층 단위의 MAX, AVG)"""
    service = AggregationService(db)
    request = AggregationRequest(
        factory=factory,
        building=building,
        floor=floor,
        start_date=start_date,
        end_date=end_date
    )
    try:
        return await service.get_temperature_aggregation(request)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


