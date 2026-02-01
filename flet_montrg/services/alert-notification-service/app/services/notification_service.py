"""
Alert Notification service layer
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, and_, or_
from typing import List, Optional
from datetime import datetime, timezone

from app.models.database_models import AlertNotification
from app.models.schemas import NotificationCreate, NotificationUpdate, NotificationStatus
from app.core.logging import get_logger

logger = get_logger(__name__)


class NotificationService:
    """알림 발송 서비스"""
    
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_notifications(
        self,
        skip: int = 0,
        limit: int = 100,
        alert_id: Optional[int] = None,
        subscription_id: Optional[int] = None,
        status: Optional[NotificationStatus] = None,
        notify_type: Optional[str] = None
    ) -> List[AlertNotification]:
        """알림 목록 조회"""
        query = select(AlertNotification)
        
        # 필터 조건 추가
        if alert_id:
            query = query.filter(AlertNotification.alert_id == alert_id)
        if subscription_id:
            query = query.filter(AlertNotification.subscription_id == subscription_id)
        if status:
            query = query.filter(AlertNotification.status == status.value)
        if notify_type:
            query = query.filter(AlertNotification.notify_type == notify_type)
        
        query = query.order_by(desc(AlertNotification.notification_id)).offset(skip).limit(limit)
        
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_notification_by_id(self, notification_id: int) -> Optional[AlertNotification]:
        """ID로 알림 조회"""
        result = await self.db.execute(
            select(AlertNotification).filter(AlertNotification.notification_id == notification_id)
        )
        return result.scalar_one_or_none()

    async def get_notifications_by_alert_id(self, alert_id: int) -> List[AlertNotification]:
        """알람별 알림 목록 조회"""
        result = await self.db.execute(
            select(AlertNotification)
            .filter(AlertNotification.alert_id == alert_id)
            .order_by(desc(AlertNotification.notification_id))
        )
        return list(result.scalars().all())

    async def get_notifications_by_subscription_id(self, subscription_id: int) -> List[AlertNotification]:
        """구독별 알림 목록 조회"""
        result = await self.db.execute(
            select(AlertNotification)
            .filter(AlertNotification.subscription_id == subscription_id)
            .order_by(desc(AlertNotification.notification_id))
        )
        return list(result.scalars().all())

    async def get_pending_notifications(self, limit: int = 100) -> List[AlertNotification]:
        """대기 중인 알림 목록 조회"""
        result = await self.db.execute(
            select(AlertNotification)
            .filter(AlertNotification.status == NotificationStatus.PENDING.value)
            .order_by(AlertNotification.created_time)
            .limit(limit)
        )
        return list(result.scalars().all())

    async def get_failed_notifications(self, limit: int = 100) -> List[AlertNotification]:
        """실패한 알림 목록 조회"""
        result = await self.db.execute(
            select(AlertNotification)
            .filter(AlertNotification.status == NotificationStatus.FAILED.value)
            .order_by(desc(AlertNotification.last_try_time))
            .limit(limit)
        )
        return list(result.scalars().all())

    async def create_notification(self, notification: NotificationCreate) -> AlertNotification:
        """새 알림 생성"""
        notification_data = notification.model_dump(exclude_unset=True)
        
        # created_time이 없으면 현재 시간 설정 (UTC)
        if "created_time" not in notification_data:
            notification_data["created_time"] = datetime.now(timezone.utc)
        
        db_notification = AlertNotification(**notification_data)
        self.db.add(db_notification)
        await self.db.commit()
        await self.db.refresh(db_notification)
        
        logger.info(f"Notification created: notification_id={db_notification.notification_id}, alert_id={db_notification.alert_id}")
        return db_notification

    async def update_notification(
        self,
        notification_id: int,
        notification_update: NotificationUpdate
    ) -> Optional[AlertNotification]:
        """알림 수정"""
        db_notification = await self.get_notification_by_id(notification_id)
        if not db_notification:
            return None
        
        update_data = notification_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_notification, field, value)
        
        await self.db.commit()
        await self.db.refresh(db_notification)
        
        logger.info(f"Notification updated: notification_id={notification_id}")
        return db_notification

    async def mark_as_sent(self, notification_id: int) -> Optional[AlertNotification]:
        """알림을 발송 완료로 표시"""
        db_notification = await self.get_notification_by_id(notification_id)
        if not db_notification:
            return None
        
        db_notification.status = NotificationStatus.SENT.value
        db_notification.sent_time = datetime.now(timezone.utc)
        
        await self.db.commit()
        await self.db.refresh(db_notification)
        
        logger.info(f"Notification marked as sent: notification_id={notification_id}")
        return db_notification

    async def mark_as_failed(
        self,
        notification_id: int,
        fail_reason: str,
        increment_try: bool = True
    ) -> Optional[AlertNotification]:
        """알림을 실패로 표시"""
        db_notification = await self.get_notification_by_id(notification_id)
        if not db_notification:
            return None
        
        db_notification.status = NotificationStatus.FAILED.value
        db_notification.fail_reason = fail_reason
        db_notification.last_try_time = datetime.now(timezone.utc)
        
        if increment_try:
            db_notification.try_count += 1
        
        await self.db.commit()
        await self.db.refresh(db_notification)
        
        logger.info(f"Notification marked as failed: notification_id={notification_id}, reason={fail_reason}")
        return db_notification

    async def mark_as_retrying(self, notification_id: int) -> Optional[AlertNotification]:
        """알림을 재시도 중으로 표시"""
        db_notification = await self.get_notification_by_id(notification_id)
        if not db_notification:
            return None
        
        db_notification.status = NotificationStatus.RETRYING.value
        db_notification.last_try_time = datetime.now(timezone.utc)
        db_notification.try_count += 1
        
        await self.db.commit()
        await self.db.refresh(db_notification)
        
        logger.info(f"Notification marked as retrying: notification_id={notification_id}")
        return db_notification

    async def delete_notification(self, notification_id: int) -> bool:
        """알림 삭제"""
        db_notification = await self.get_notification_by_id(notification_id)
        if not db_notification:
            return False
        
        await self.db.delete(db_notification)
        await self.db.commit()
        
        logger.info(f"Notification deleted: notification_id={notification_id}")
        return True
