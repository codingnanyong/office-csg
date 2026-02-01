"""
Alert service layer
"""
from collections import defaultdict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from typing import List, Optional
from datetime import datetime, date, timezone, timedelta

from app.models.database_models import Alert
from app.models.schemas import AlertCreate
from app.core.logging import get_logger
from app.clients.location_client import location_client
from app.clients.alert_subscription_client import alert_subscription_client
from app.clients.alert_notification_client import alert_notification_client

logger = get_logger(__name__)


class AlertService:
    """알람 서비스"""
    
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_alerts(
        self, 
        skip: int = 0, 
        limit: int = 100,
        sensor_id: Optional[str] = None,
        loc_id: Optional[str] = None,
        alert_type: Optional[str] = None,
        alert_level: Optional[str] = None,
        threshold_id: Optional[int] = None
    ) -> List[Alert]:
        """알람 목록 조회"""
        query = select(Alert)
        
        # 필터 조건을 개별적으로 추가 (and_ 사용 시 문제 방지)
        if sensor_id:
            query = query.filter(Alert.sensor_id == sensor_id)
        if loc_id:
            query = query.filter(Alert.loc_id == loc_id)
        if alert_type:
            query = query.filter(Alert.alert_type == alert_type)
        if alert_level:
            query = query.filter(Alert.alert_level == alert_level)
        if threshold_id:
            query = query.filter(Alert.threshold_id == threshold_id)
        
        query = query.order_by(desc(Alert.alert_time)).offset(skip).limit(limit)
        
        result = await self.db.execute(query)
        return list(result.scalars().all())
    
    async def get_today_alerts(
        self,
        skip: int = 0,
        limit: int = 100,
        sensor_id: Optional[str] = None,
        loc_id: Optional[str] = None,
        alert_type: Optional[str] = None,
        alert_level: Optional[str] = None
    ) -> List[Alert]:
        """오늘 알람 목록 조회"""
        query = select(Alert)
        
        # 오늘 날짜 필터 (날짜 범위로 비교 - SQLite 호환)
        today = date.today()
        today_start = datetime.combine(today, datetime.min.time())
        today_end = datetime.combine(today, datetime.max.time())
        
        # 조건을 개별적으로 추가 (and_ 사용 시 문제 방지)
        query = query.filter(Alert.alert_time >= today_start)
        query = query.filter(Alert.alert_time <= today_end)
        
        # 추가 필터 조건
        if sensor_id:
            query = query.filter(Alert.sensor_id == sensor_id)
        if loc_id:
            query = query.filter(Alert.loc_id == loc_id)
        if alert_type:
            query = query.filter(Alert.alert_type == alert_type)
        if alert_level:
            query = query.filter(Alert.alert_level == alert_level)
        
        query = query.order_by(desc(Alert.alert_time)).offset(skip).limit(limit)
        
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_alert_by_id(self, alert_id: int) -> Optional[Alert]:
        """ID로 알람 조회"""
        result = await self.db.execute(
            select(Alert).filter(Alert.alert_id == alert_id)
        )
        return result.scalar_one_or_none()


    async def create_alert(self, alert: AlertCreate) -> Alert:
        """새 알람 생성 및 구독자에게 알림 생성"""
        alert_data = alert.model_dump(exclude_unset=True)
        
        # alert_time이 없으면 현재 시간 사용 (UTC)
        if "alert_time" not in alert_data or alert_data["alert_time"] is None:
            alert_data["alert_time"] = datetime.now(timezone.utc)
        
        db_alert = Alert(**alert_data)
        self.db.add(db_alert)
        await self.db.commit()
        await self.db.refresh(db_alert)
        
        logger.info(f"Alert created: alert_id={db_alert.alert_id}, sensor_id={db_alert.sensor_id}, level={db_alert.alert_level}")
        
        # 알람 생성 후 구독자 조회 및 알림 생성 (비동기로 처리, 실패해도 알람 생성은 성공)
        try:
            await self._create_notifications_for_alert(db_alert)
        except Exception as e:
            logger.error(f"Failed to create notifications for alert {db_alert.alert_id}: {e}", exc_info=True)
        
        return db_alert
    
    async def _create_notifications_for_alert(self, alert: Alert) -> None:
        """알람에 대한 구독자 조회 및 알림 생성"""
        # 1. 위치 정보 가져오기 (loc_id 또는 sensor_id로)
        location = None
        if alert.loc_id:
            location = await location_client.get_location_by_loc_id(alert.loc_id)
        elif alert.sensor_id:
            location = await location_client.get_location_by_sensor_id(alert.sensor_id)
        
        # 2. 모든 활성화된 구독 조회 (threshold_type 필터링)
        all_subscriptions = await alert_subscription_client.get_matching_subscriptions(
            threshold_type=alert.threshold_type,
            enabled=True
        )
        
        # 3. 계층적 와일드카드 매칭 로직 적용
        # NULL은 "전체"를 의미하는 와일드카드
        # factory만 지정 → 해당 factory 전체
        # factory+building → 특정 building만
        # factory+building+floor → 특정 층만
        # factory+building+floor+area → 특정 구역만
        matching_subscriptions = []
        matched_subscription_ids = set()  # 중복 제거용
        
        alert_factory = location.get("factory") if location else None
        alert_building = location.get("building") if location else None
        alert_floor = location.get("floor") if location else None
        alert_area = location.get("area") if location else None
        alert_sensor_id = alert.sensor_id
        
        for sub in all_subscriptions:
            sub_id = sub.get("subscription_id")
            if not sub_id:
                continue
            
            # 이미 매칭된 구독은 스킵 (중복 제거)
            if sub_id in matched_subscription_ids:
                continue
            
            # sensor_id가 정확히 일치하는 경우
            if alert_sensor_id and sub.get("sensor_id") == alert_sensor_id:
                matching_subscriptions.append(sub)
                matched_subscription_ids.add(sub_id)
                continue
            
            # sensor_id가 구독에 지정되어 있고 alert의 sensor_id와 다르면 스킵
            if sub.get("sensor_id") and alert_sensor_id and sub.get("sensor_id") != alert_sensor_id:
                continue
            
            # 위치 계층적 매칭
            sub_factory = sub.get("factory")
            sub_building = sub.get("building")
            sub_floor = sub.get("floor")
            sub_area = sub.get("area")
            
            # factory 매칭
            if sub_factory:
                if alert_factory != sub_factory:
                    continue  # factory가 다르면 매칭 안됨
            # sub_factory가 NULL이면 모든 factory에 대해 와일드카드
            
            # building 매칭
            if sub_building:
                if alert_building != sub_building:
                    continue  # building이 다르면 매칭 안됨
            # sub_building이 NULL이면 해당 factory의 모든 building에 대해 와일드카드
            
            # floor 매칭
            if sub_floor is not None:
                if alert_floor != sub_floor:
                    continue  # floor가 다르면 매칭 안됨
            # sub_floor가 NULL이면 해당 building의 모든 floor에 대해 와일드카드
            
            # area 매칭
            if sub_area:
                if alert_area != sub_area:
                    continue  # area가 다르면 매칭 안됨
            # sub_area가 NULL이면 해당 floor의 모든 area에 대해 와일드카드
            
            # 모든 조건을 통과하면 매칭됨
            matching_subscriptions.append(sub)
            matched_subscription_ids.add(sub_id)
        
        # 4. alert_level과 정확히 일치하거나 더 높은 min_level의 구독만 필터링
        # alert_level 우선순위: green < yellow < orange < red < critical
        # 상위 레벨만 매칭: alert_level >= min_level이면 매칭되지만,
        # 여러 구독이 매칭될 경우 가장 높은 min_level의 구독만 선택
        level_priority = {
            "green": 1,
            "yellow": 2,
            "orange": 3,
            "red": 4,
            "critical": 5,
            "low": 1,
            "medium": 2,
            "high": 3,
            "warning": 2,
            "info": 1
        }
        
        alert_level_priority = level_priority.get(alert.alert_level.lower(), 0)
        
        # 먼저 alert_level >= min_level 조건으로 필터링
        candidate_subscriptions = []
        for sub in matching_subscriptions:
            min_level = sub.get("min_level")
            if not min_level:
                # min_level이 없으면 모든 레벨에 대해 알림 (후보에 포함)
                candidate_subscriptions.append(sub)
            else:
                min_level_priority = level_priority.get(min_level.lower(), 0)
                if alert_level_priority >= min_level_priority:
                    candidate_subscriptions.append(sub)
        
        # 매칭된 구독 중 가장 높은 min_level만 선택
        # 같은 subscriber+notify_id 조합에 대해 가장 높은 min_level의 구독만 선택
        if not candidate_subscriptions:
            level_filtered_subscriptions = []
        else:
            # subscriber+notify_id 조합별로 그룹화
            subscriptions_by_recipient = defaultdict(list)
            
            for sub in candidate_subscriptions:
                subscriber = sub.get("subscriber", "")
                notify_id = sub.get("notify_id", "")
                key = (subscriber, notify_id)
                subscriptions_by_recipient[key].append(sub)
            
            level_filtered_subscriptions = []
            
            # 각 recipient 그룹에서 가장 높은 min_level의 구독만 선택
            for recipient_key, subs in subscriptions_by_recipient.items():
                # min_level이 있는 구독만 필터링
                subs_with_level = [s for s in subs if s.get("min_level")]
                
                if subs_with_level:
                    # 가장 높은 min_level 찾기
                    max_min_level_priority = max(
                        level_priority.get(s.get("min_level", "").lower(), 0)
                        for s in subs_with_level
                    )
                    # 가장 높은 min_level과 일치하는 구독만 선택
                    selected = [
                        s for s in subs_with_level
                        if level_priority.get(s.get("min_level", "").lower(), 0) == max_min_level_priority
                    ]
                    # 같은 min_level이 여러 개면 첫 번째만 선택
                    level_filtered_subscriptions.append(selected[0])
                else:
                    # min_level이 없는 구독만 있는 경우 첫 번째만 선택
                    level_filtered_subscriptions.append(subs[0])
        
        # 5. 매칭된 구독자들에 대해 notification 생성
        created_count = 0
        for subscription in level_filtered_subscriptions:
            try:
                notification = await alert_notification_client.create_notification(
                    alert_id=alert.alert_id,
                    subscription_id=subscription["subscription_id"],
                    notify_type=subscription["notify_type"],
                    notify_id=subscription["notify_id"]
                )
                if notification:
                    created_count += 1
                    logger.info(f"Notification created: notification_id={notification.get('notification_id')}, subscription_id={subscription['subscription_id']}")
            except Exception as e:
                logger.error(f"Failed to create notification for subscription {subscription['subscription_id']}: {e}")
        
        logger.info(f"Created {created_count} notifications for alert {alert.alert_id} (matched {len(level_filtered_subscriptions)} subscriptions from {len(matching_subscriptions)} location-matched)")