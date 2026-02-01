"""
Sensor Threshold Mapping API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import logging

from app.core.database import get_db
from app.models.schemas import MappingCreate, MappingUpdate, Mapping
from app.services.mapping_service import MappingService

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/", response_model=Mapping, status_code=201)
async def create_mapping(
    mapping: MappingCreate,
    db: AsyncSession = Depends(get_db)
):
    """새 매핑 생성"""
    try:
        service = MappingService(db)
        return await service.create_mapping(mapping)
    except Exception as e:
        logger.error(f"Error creating mapping: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.get("/", response_model=List[Mapping])
async def get_mappings(
    skip: int = Query(0, ge=0, description="건너뛸 레코드 수"),
    limit: int = Query(100, ge=1, le=1000, description="가져올 레코드 수"),
    sensor_id: Optional[str] = Query(None, description="센서 ID로 필터링"),
    threshold_id: Optional[int] = Query(None, description="임계치 ID로 필터링"),
    enabled: Optional[bool] = Query(None, description="활성화 여부로 필터링"),
    db: AsyncSession = Depends(get_db)
):
    """매핑 목록 조회"""
    try:
        service = MappingService(db)
        mappings = await service.get_mappings(
            skip=skip,
            limit=limit,
            sensor_id=sensor_id,
            threshold_id=threshold_id,
            enabled=enabled
        )
        return mappings
    except Exception as e:
        logger.error(f"Error getting mappings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.get("/{map_id}", response_model=Mapping)
async def get_mapping_by_id(
    map_id: int,
    db: AsyncSession = Depends(get_db)
):
    """매핑 상세 조회"""
    try:
        service = MappingService(db)
        mapping = await service.get_mapping_by_id(map_id)
        if not mapping:
            raise HTTPException(status_code=404, detail=f"Mapping with id {map_id} not found")
        return mapping
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting mapping: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.get("/sensor/{sensor_id}", response_model=List[Mapping])
async def get_mappings_by_sensor_id(
    sensor_id: str,
    enabled: bool = Query(True, description="활성화된 매핑만 조회"),
    db: AsyncSession = Depends(get_db)
):
    """센서별 매핑 목록 조회"""
    try:
        service = MappingService(db)
        mappings = await service.get_mappings_by_sensor_id(sensor_id, enabled=enabled)
        return mappings
    except Exception as e:
        logger.error(f"Error getting mappings by sensor_id: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.get("/threshold/{threshold_id}", response_model=List[Mapping])
async def get_mappings_by_threshold_id(
    threshold_id: int,
    enabled: bool = Query(True, description="활성화된 매핑만 조회"),
    db: AsyncSession = Depends(get_db)
):
    """임계치별 매핑 목록 조회"""
    try:
        service = MappingService(db)
        mappings = await service.get_mappings_by_threshold_id(threshold_id, enabled=enabled)
        return mappings
    except Exception as e:
        logger.error(f"Error getting mappings by threshold_id: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.put("/{map_id}", response_model=Mapping)
async def update_mapping(
    map_id: int,
    mapping_update: MappingUpdate,
    db: AsyncSession = Depends(get_db)
):
    """매핑 수정"""
    try:
        service = MappingService(db)
        mapping = await service.update_mapping(map_id, mapping_update)
        if not mapping:
            raise HTTPException(status_code=404, detail=f"Mapping with id {map_id} not found")
        return mapping
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating mapping: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.delete("/{map_id}", status_code=204)
async def delete_mapping(
    map_id: int,
    db: AsyncSession = Depends(get_db)
):
    """매핑 삭제"""
    try:
        service = MappingService(db)
        success = await service.delete_mapping(map_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Mapping with id {map_id} not found")
        return None
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting mapping: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.post("/{map_id}/enable", response_model=Mapping)
async def enable_mapping(
    map_id: int,
    db: AsyncSession = Depends(get_db)
):
    """매핑 활성화"""
    try:
        service = MappingService(db)
        mapping = await service.enable_mapping(map_id)
        if not mapping:
            raise HTTPException(status_code=404, detail=f"Mapping with id {map_id} not found")
        return mapping
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error enabling mapping: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.post("/{map_id}/disable", response_model=Mapping)
async def disable_mapping(
    map_id: int,
    db: AsyncSession = Depends(get_db)
):
    """매핑 비활성화"""
    try:
        service = MappingService(db)
        mapping = await service.disable_mapping(map_id)
        if not mapping:
            raise HTTPException(status_code=404, detail=f"Mapping with id {map_id} not found")
        return mapping
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error disabling mapping: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
