"""
Alert Subscription API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import logging

from app.core.database import get_db
from app.models.schemas import SubscriptionCreate, SubscriptionUpdate, Subscription
from app.services.subscription_service import SubscriptionService

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/", response_model=Subscription, status_code=201)
async def create_subscription(
    subscription: SubscriptionCreate,
    db: AsyncSession = Depends(get_db)
):
    """새 구독 생성"""
    try:
        service = SubscriptionService(db)
        return await service.create_subscription(subscription)
    except Exception as e:
        logger.error(f"Error creating subscription: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.get("/", response_model=List[Subscription])
async def get_subscriptions(
    skip: int = Query(0, ge=0, description="건너뛸 레코드 수"),
    limit: int = Query(100, ge=1, le=1000, description="가져올 레코드 수"),
    subscriber: Optional[str] = Query(None, description="구독자 이름으로 필터링"),
    plant: Optional[str] = Query(None, description="공장으로 필터링"),
    factory: Optional[str] = Query(None, description="공장명으로 필터링"),
    building: Optional[str] = Query(None, description="건물로 필터링"),
    floor: Optional[int] = Query(None, description="층으로 필터링"),
    area: Optional[str] = Query(None, description="구역으로 필터링"),
    sensor_id: Optional[str] = Query(None, description="센서 ID로 필터링"),
    threshold_type: Optional[str] = Query(None, description="임계치 타입으로 필터링"),
    enabled: Optional[bool] = Query(None, description="활성화 여부로 필터링"),
    db: AsyncSession = Depends(get_db)
):
    """구독 목록 조회"""
    try:
        service = SubscriptionService(db)
        subscriptions = await service.get_subscriptions(
            skip=skip,
            limit=limit,
            subscriber=subscriber,
            plant=plant,
            factory=factory,
            building=building,
            floor=floor,
            area=area,
            sensor_id=sensor_id,
            threshold_type=threshold_type,
            enabled=enabled
        )
        return subscriptions
    except Exception as e:
        logger.error(f"Error getting subscriptions: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.get("/subscriber/{subscriber}", response_model=List[Subscription])
async def get_subscriptions_by_subscriber(
    subscriber: str,
    db: AsyncSession = Depends(get_db)
):
    """구독자별 구독 목록 조회"""
    try:
        service = SubscriptionService(db)
        subscriptions = await service.get_subscriptions_by_subscriber(subscriber)
        return subscriptions
    except Exception as e:
        logger.error(f"Error getting subscriptions by subscriber: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.get("/{subscription_id}", response_model=Subscription)
async def get_subscription(
    subscription_id: int,
    db: AsyncSession = Depends(get_db)
):
    """특정 구독 조회"""
    try:
        service = SubscriptionService(db)
        subscription = await service.get_subscription_by_id(subscription_id)
        if not subscription:
            raise HTTPException(status_code=404, detail="Subscription not found")
        return subscription
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting subscription: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.put("/{subscription_id}", response_model=Subscription)
async def update_subscription(
    subscription_id: int,
    subscription_update: SubscriptionUpdate,
    db: AsyncSession = Depends(get_db)
):
    """구독 수정"""
    try:
        service = SubscriptionService(db)
        subscription = await service.update_subscription(subscription_id, subscription_update)
        if not subscription:
            raise HTTPException(status_code=404, detail="Subscription not found")
        return subscription
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating subscription: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.delete("/{subscription_id}", status_code=200)
async def delete_subscription(
    subscription_id: int,
    db: AsyncSession = Depends(get_db)
):
    """구독 삭제"""
    try:
        service = SubscriptionService(db)
        deleted = await service.delete_subscription(subscription_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Subscription not found")
        return {"message": f"Subscription {subscription_id} deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting subscription: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.post("/{subscription_id}/enable", response_model=Subscription)
async def enable_subscription(
    subscription_id: int,
    db: AsyncSession = Depends(get_db)
):
    """구독 활성화"""
    try:
        service = SubscriptionService(db)
        subscription = await service.enable_subscription(subscription_id)
        if not subscription:
            raise HTTPException(status_code=404, detail="Subscription not found")
        return subscription
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error enabling subscription: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.post("/{subscription_id}/disable", response_model=Subscription)
async def disable_subscription(
    subscription_id: int,
    db: AsyncSession = Depends(get_db)
):
    """구독 비활성화"""
    try:
        service = SubscriptionService(db)
        subscription = await service.disable_subscription(subscription_id)
        if not subscription:
            raise HTTPException(status_code=404, detail="Subscription not found")
        return subscription
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error disabling subscription: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
