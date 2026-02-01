"""
Pydantic schemas for API requests and responses
"""
from pydantic import BaseModel, Field, field_validator, field_serializer
from typing import Optional
from enum import Enum
from datetime import datetime, timezone, timedelta


class NotifyType(str, Enum):
    """알림 타입 열거형"""
    EMAIL = "email"
    KAKAO = "kakao"
    SMS = "sms"
    APP = "app"


class SubscriptionBase(BaseModel):
    """구독 기본 모델"""
    plant: Optional[str] = Field(None, description="공장")
    factory: Optional[str] = Field(None, description="공장명")
    building: Optional[str] = Field(None, description="건물")
    floor: Optional[int] = Field(None, description="층")
    area: Optional[str] = Field(None, description="구역")
    sensor_id: Optional[str] = Field(None, description="센서 ID")
    threshold_type: Optional[str] = Field(None, description="임계치 타입")
    min_level: Optional[str] = Field(None, description="최소 알람 레벨")
    subscriber: str = Field(..., description="구독자 이름")
    notify_type: NotifyType = Field(NotifyType.EMAIL, description="알림 타입 (email, kakao, sms, app)")
    notify_id: str = Field(..., description="알림 ID (email이면 이메일 주소, app이면 계정 이름)")
    enabled: bool = Field(True, description="활성화 여부")


class SubscriptionCreate(SubscriptionBase):
    """구독 생성 모델"""
    pass


class SubscriptionUpdate(BaseModel):
    """구독 수정 모델"""
    plant: Optional[str] = None
    factory: Optional[str] = None
    building: Optional[str] = None
    floor: Optional[int] = None
    area: Optional[str] = None
    sensor_id: Optional[str] = None
    threshold_type: Optional[str] = None
    min_level: Optional[str] = None
    subscriber: Optional[str] = None
    notify_type: Optional[NotifyType] = None
    notify_id: Optional[str] = None
    enabled: Optional[bool] = None


class Subscription(SubscriptionBase):
    """구독 응답 모델"""
    subscription_id: int = Field(..., description="구독 ID")
    upd_dt: datetime = Field(..., description="수정 시간")
    
    @field_serializer('upd_dt')
    def serialize_upd_dt(self, value: datetime) -> str:
        """UTC 시간을 KST(UTC+9)로 변환하여 반환"""
        if value is None:
            return None
        # UTC 시간대가 없으면 UTC로 가정
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        # KST로 변환 (UTC+9)
        kst = timezone(timedelta(hours=9))
        kst_time = value.astimezone(kst)
        return kst_time.isoformat()
    
    model_config = {
        "from_attributes": True
    }
