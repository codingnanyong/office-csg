"""
Alert Notification API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import logging

from app.core.database import get_db
from app.models.schemas import NotificationCreate, NotificationUpdate, Notification, NotificationStatus
from app.services.notification_service import NotificationService

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/", response_model=Notification, status_code=201)
async def create_notification(
    notification: NotificationCreate,
    db: AsyncSession = Depends(get_db)
):
    """새 알림 생성"""
    try:
        service = NotificationService(db)
        return await service.create_notification(notification)
    except Exception as e:
        logger.error(f"Error creating notification: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.get("/", response_model=List[Notification])
async def get_notifications(
    skip: int = Query(0, ge=0, description="건너뛸 레코드 수"),
    limit: int = Query(100, ge=1, le=1000, description="가져올 레코드 수"),
    alert_id: Optional[int] = Query(None, description="알람 ID로 필터링"),
    subscription_id: Optional[int] = Query(None, description="구독 ID로 필터링"),
    status: Optional[NotificationStatus] = Query(None, description="상태로 필터링"),
    notify_type: Optional[str] = Query(None, description="알림 타입으로 필터링"),
    db: AsyncSession = Depends(get_db)
):
    """알림 목록 조회"""
    try:
        service = NotificationService(db)
        notifications = await service.get_notifications(
            skip=skip,
            limit=limit,
            alert_id=alert_id,
            subscription_id=subscription_id,
            status=status,
            notify_type=notify_type
        )
        return notifications
    except Exception as e:
        logger.error(f"Error getting notifications: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.get("/alert/{alert_id}", response_model=List[Notification])
async def get_notifications_by_alert(
    alert_id: int,
    db: AsyncSession = Depends(get_db)
):
    """알람별 알림 목록 조회"""
    try:
        service = NotificationService(db)
        notifications = await service.get_notifications_by_alert_id(alert_id)
        return notifications
    except Exception as e:
        logger.error(f"Error getting notifications by alert: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.get("/subscription/{subscription_id}", response_model=List[Notification])
async def get_notifications_by_subscription(
    subscription_id: int,
    db: AsyncSession = Depends(get_db)
):
    """구독별 알림 목록 조회"""
    try:
        service = NotificationService(db)
        notifications = await service.get_notifications_by_subscription_id(subscription_id)
        return notifications
    except Exception as e:
        logger.error(f"Error getting notifications by subscription: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.get("/pending", response_model=List[Notification])
async def get_pending_notifications(
    limit: int = Query(100, ge=1, le=1000, description="가져올 레코드 수"),
    db: AsyncSession = Depends(get_db)
):
    """대기 중인 알림 목록 조회"""
    try:
        service = NotificationService(db)
        notifications = await service.get_pending_notifications(limit=limit)
        return notifications
    except Exception as e:
        logger.error(f"Error getting pending notifications: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.get("/failed", response_model=List[Notification])
async def get_failed_notifications(
    limit: int = Query(100, ge=1, le=1000, description="가져올 레코드 수"),
    db: AsyncSession = Depends(get_db)
):
    """실패한 알림 목록 조회"""
    try:
        service = NotificationService(db)
        notifications = await service.get_failed_notifications(limit=limit)
        return notifications
    except Exception as e:
        logger.error(f"Error getting failed notifications: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.get("/{notification_id}", response_model=Notification)
async def get_notification(
    notification_id: int,
    db: AsyncSession = Depends(get_db)
):
    """특정 알림 조회"""
    try:
        service = NotificationService(db)
        notification = await service.get_notification_by_id(notification_id)
        if not notification:
            raise HTTPException(status_code=404, detail="Notification not found")
        return notification
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting notification: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.put("/{notification_id}", response_model=Notification)
async def update_notification(
    notification_id: int,
    notification_update: NotificationUpdate,
    db: AsyncSession = Depends(get_db)
):
    """알림 수정"""
    try:
        service = NotificationService(db)
        notification = await service.update_notification(notification_id, notification_update)
        if not notification:
            raise HTTPException(status_code=404, detail="Notification not found")
        return notification
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating notification: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.post("/{notification_id}/mark-sent", response_model=Notification)
async def mark_notification_as_sent(
    notification_id: int,
    db: AsyncSession = Depends(get_db)
):
    """알림을 발송 완료로 표시"""
    try:
        service = NotificationService(db)
        notification = await service.mark_as_sent(notification_id)
        if not notification:
            raise HTTPException(status_code=404, detail="Notification not found")
        return notification
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error marking notification as sent: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.post("/{notification_id}/mark-failed", response_model=Notification)
async def mark_notification_as_failed(
    notification_id: int,
    fail_reason: str = Query(..., description="실패 사유"),
    increment_try: bool = Query(True, description="재시도 횟수 증가 여부"),
    db: AsyncSession = Depends(get_db)
):
    """알림을 실패로 표시"""
    try:
        service = NotificationService(db)
        notification = await service.mark_as_failed(notification_id, fail_reason, increment_try)
        if not notification:
            raise HTTPException(status_code=404, detail="Notification not found")
        return notification
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error marking notification as failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.post("/{notification_id}/mark-retrying", response_model=Notification)
async def mark_notification_as_retrying(
    notification_id: int,
    db: AsyncSession = Depends(get_db)
):
    """알림을 재시도 중으로 표시"""
    try:
        service = NotificationService(db)
        notification = await service.mark_as_retrying(notification_id)
        if not notification:
            raise HTTPException(status_code=404, detail="Notification not found")
        return notification
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error marking notification as retrying: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@router.delete("/{notification_id}", status_code=200)
async def delete_notification(
    notification_id: int,
    db: AsyncSession = Depends(get_db)
):
    """알림 삭제"""
    try:
        service = NotificationService(db)
        deleted = await service.delete_notification(notification_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Notification not found")
        return {"message": f"Notification {notification_id} deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting notification: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
