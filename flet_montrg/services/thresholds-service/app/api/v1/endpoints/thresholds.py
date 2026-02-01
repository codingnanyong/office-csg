"""
Threshold API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from app.core.database import get_db
from app.models.schemas import ThresholdCreate, ThresholdUpdate, Threshold
from app.services.threshold_service import ThresholdService

router = APIRouter()


@router.get("/", response_model=List[Threshold])
async def get_thresholds(
    skip: int = Query(0, ge=0, description="건너뛸 레코드 수"),
    limit: int = Query(100, ge=1, le=1000, description="가져올 레코드 수"),
    db: AsyncSession = Depends(get_db)
):
    """모든 임계치 조회"""
    service = ThresholdService(db)
    return await service.get_thresholds(skip=skip, limit=limit)


@router.get("/{threshold_id}", response_model=Threshold)
async def get_threshold(threshold_id: int, db: AsyncSession = Depends(get_db)):
    """특정 임계치 조회"""
    service = ThresholdService(db)
    threshold = await service.get_threshold_by_id(threshold_id)
    if not threshold:
        raise HTTPException(status_code=404, detail="Threshold not found")
    return threshold


@router.post("/", response_model=Threshold, status_code=201)
async def create_threshold(threshold: ThresholdCreate, db: AsyncSession = Depends(get_db)):
    """새 임계치 생성"""
    service = ThresholdService(db)
    return await service.create_threshold(threshold)


@router.put("/{threshold_id}", response_model=Threshold)
async def update_threshold(
    threshold_id: int, 
    threshold_update: ThresholdUpdate, 
    db: AsyncSession = Depends(get_db)
):
    """임계치 수정"""
    service = ThresholdService(db)
    threshold = await service.update_threshold(threshold_id, threshold_update)
    if not threshold:
        raise HTTPException(status_code=404, detail="Threshold not found")
    return threshold


@router.delete("/{threshold_id}")
async def delete_threshold(threshold_id: int, db: AsyncSession = Depends(get_db)):
    """임계치 삭제"""
    service = ThresholdService(db)
    threshold = await service.delete_threshold(threshold_id)
    if not threshold:
        raise HTTPException(status_code=404, detail="Threshold not found")
    return {"message": f"Threshold {threshold_id} deleted", "deleted": threshold}


@router.get("/type/{threshold_type}", response_model=List[Threshold])
async def get_thresholds_by_type(threshold_type: str, db: AsyncSession = Depends(get_db)):
    """타입별 임계치 조회"""
    service = ThresholdService(db)
    return await service.get_thresholds_by_type(threshold_type)