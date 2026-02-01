"""
Alert API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional

from app.core.database import get_db
from app.models.schemas import AlertCreate, Alert
from app.services.alert_service import AlertService

router = APIRouter()


@router.post("/", response_model=Alert, status_code=201)
async def create_alert(alert: AlertCreate, db: AsyncSession = Depends(get_db)):
    """새 알람 생성"""
    service = AlertService(db)
    return await service.create_alert(alert)


@router.get("/", response_model=List[Alert])
async def get_alerts(
    skip: int = Query(0, ge=0, description="건너뛸 레코드 수"),
    limit: int = Query(100, ge=1, le=1000, description="가져올 레코드 수"),
    sensor_id: Optional[str] = Query(None, description="센서 ID로 필터링"),
    loc_id: Optional[str] = Query(None, description="위치 ID로 필터링"),
    alert_type: Optional[str] = Query(None, description="알람 타입으로 필터링"),
    alert_level: Optional[str] = Query(None, description="알람 레벨로 필터링"),
    threshold_id: Optional[int] = Query(None, description="임계치 ID로 필터링"),
    db: AsyncSession = Depends(get_db)
):
    """알람 목록 조회"""
    try:
        service = AlertService(db)
        alerts = await service.get_alerts(
            skip=skip,
            limit=limit,
            sensor_id=sensor_id,
            loc_id=loc_id,
            alert_type=alert_type,
            alert_level=alert_level,
            threshold_id=threshold_id
        )
        return alerts
    except Exception as e:
        from app.core.logging import get_logger
        logger = get_logger(__name__)
        logger.error(f"Error getting alerts: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/today", response_model=List[Alert])
async def get_today_alerts(
    skip: int = Query(0, ge=0, description="건너뛸 레코드 수"),
    limit: int = Query(100, ge=1, le=1000, description="가져올 레코드 수"),
    sensor_id: Optional[str] = Query(None, description="센서 ID로 필터링"),
    loc_id: Optional[str] = Query(None, description="위치 ID로 필터링"),
    alert_type: Optional[str] = Query(None, description="알람 타입으로 필터링"),
    alert_level: Optional[str] = Query(None, description="알람 레벨로 필터링"),
    db: AsyncSession = Depends(get_db)
):
    """오늘 알람 목록 조회"""
    try:
        service = AlertService(db)
        alerts = await service.get_today_alerts(
            skip=skip,
            limit=limit,
            sensor_id=sensor_id,
            loc_id=loc_id,
            alert_type=alert_type,
            alert_level=alert_level
        )
        return alerts
    except Exception as e:
        from app.core.logging import get_logger
        logger = get_logger(__name__)
        logger.error(f"Error getting today alerts: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/{alert_id}", response_model=Alert)
async def get_alert(alert_id: int, db: AsyncSession = Depends(get_db)):
    """특정 알람 조회"""
    service = AlertService(db)
    alert = await service.get_alert_by_id(alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert
