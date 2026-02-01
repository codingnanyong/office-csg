"""
Alert Subscription service layer
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, and_, or_
from typing import List, Optional

from app.models.database_models import AlertSubscription
from app.models.schemas import SubscriptionCreate, SubscriptionUpdate
from app.core.logging import get_logger

logger = get_logger(__name__)


class SubscriptionService:
    """알람 구독 서비스"""
    
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_subscriptions(
        self,
        skip: int = 0,
        limit: int = 100,
        subscriber: Optional[str] = None,
        plant: Optional[str] = None,
        factory: Optional[str] = None,
        building: Optional[str] = None,
        floor: Optional[int] = None,
        area: Optional[str] = None,
        sensor_id: Optional[str] = None,
        threshold_type: Optional[str] = None,
        enabled: Optional[bool] = None
    ) -> List[AlertSubscription]:
        """구독 목록 조회"""
        query = select(AlertSubscription)
        
        # 필터 조건 추가
        if subscriber:
            query = query.filter(AlertSubscription.subscriber == subscriber)
        if plant:
            query = query.filter(AlertSubscription.plant == plant)
        if factory:
            query = query.filter(AlertSubscription.factory == factory)
        if building:
            query = query.filter(AlertSubscription.building == building)
        if floor is not None:
            query = query.filter(AlertSubscription.floor == floor)
        if area:
            query = query.filter(AlertSubscription.area == area)
        if sensor_id:
            query = query.filter(AlertSubscription.sensor_id == sensor_id)
        if threshold_type:
            query = query.filter(AlertSubscription.threshold_type == threshold_type)
        if enabled is not None:
            query = query.filter(AlertSubscription.enabled == enabled)
        
        query = query.order_by(desc(AlertSubscription.subscription_id)).offset(skip).limit(limit)
        
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_subscription_by_id(self, subscription_id: int) -> Optional[AlertSubscription]:
        """ID로 구독 조회"""
        result = await self.db.execute(
            select(AlertSubscription).filter(AlertSubscription.subscription_id == subscription_id)
        )
        return result.scalar_one_or_none()

    async def get_subscriptions_by_subscriber(self, subscriber: str) -> List[AlertSubscription]:
        """구독자별 구독 목록 조회"""
        result = await self.db.execute(
            select(AlertSubscription)
            .filter(AlertSubscription.subscriber == subscriber)
            .filter(AlertSubscription.enabled == True)
            .order_by(desc(AlertSubscription.subscription_id))
        )
        return list(result.scalars().all())

    async def create_subscription(self, subscription: SubscriptionCreate) -> AlertSubscription:
        """새 구독 생성"""
        from datetime import datetime, timezone
        subscription_data = subscription.model_dump(exclude_unset=True)
        
        # upd_dt가 없으면 현재 시간 설정 (UTC)
        if "upd_dt" not in subscription_data:
            subscription_data["upd_dt"] = datetime.now(timezone.utc)
        
        db_subscription = AlertSubscription(**subscription_data)
        self.db.add(db_subscription)
        await self.db.commit()
        await self.db.refresh(db_subscription)
        
        logger.info(f"Subscription created: subscription_id={db_subscription.subscription_id}, subscriber={db_subscription.subscriber}")
        return db_subscription

    async def update_subscription(
        self,
        subscription_id: int,
        subscription_update: SubscriptionUpdate
    ) -> Optional[AlertSubscription]:
        """구독 수정"""
        db_subscription = await self.get_subscription_by_id(subscription_id)
        if not db_subscription:
            return None
        
        update_data = subscription_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_subscription, field, value)
        
        await self.db.commit()
        await self.db.refresh(db_subscription)
        
        logger.info(f"Subscription updated: subscription_id={subscription_id}")
        return db_subscription

    async def delete_subscription(self, subscription_id: int) -> bool:
        """구독 삭제"""
        db_subscription = await self.get_subscription_by_id(subscription_id)
        if not db_subscription:
            return False
        
        await self.db.delete(db_subscription)
        await self.db.commit()
        
        logger.info(f"Subscription deleted: subscription_id={subscription_id}")
        return True

    async def enable_subscription(self, subscription_id: int) -> Optional[AlertSubscription]:
        """구독 활성화"""
        db_subscription = await self.get_subscription_by_id(subscription_id)
        if not db_subscription:
            return None
        
        db_subscription.enabled = True
        await self.db.commit()
        await self.db.refresh(db_subscription)
        
        logger.info(f"Subscription enabled: subscription_id={subscription_id}")
        return db_subscription

    async def disable_subscription(self, subscription_id: int) -> Optional[AlertSubscription]:
        """구독 비활성화"""
        db_subscription = await self.get_subscription_by_id(subscription_id)
        if not db_subscription:
            return None
        
        db_subscription.enabled = False
        await self.db.commit()
        await self.db.refresh(db_subscription)
        
        logger.info(f"Subscription disabled: subscription_id={subscription_id}")
        return db_subscription
