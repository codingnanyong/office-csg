"""
Pydantic schemas for API requests and responses
"""
from pydantic import BaseModel, Field, field_serializer
from typing import Optional
from enum import Enum
from datetime import datetime, timezone, timedelta


class NotificationStatus(str, Enum):
    """알림 상태 열거형"""
    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


class NotifyType(str, Enum):
    """알림 타입 열거형"""
    EMAIL = "email"
    KAKAO = "kakao"
    SMS = "sms"
    APP = "app"


class NotificationBase(BaseModel):
    """알림 기본 모델"""
    alert_id: int = Field(..., description="알람 ID")
    subscription_id: int = Field(..., description="구독 ID")
    notify_type: NotifyType = Field(..., description="알림 타입 (email, kakao, sms, app)")
    notify_id: str = Field(..., description="알림 ID (email이면 이메일 주소, app이면 계정 이름)")


class NotificationCreate(NotificationBase):
    """알림 생성 모델"""
    pass


class NotificationUpdate(BaseModel):
    """알림 수정 모델"""
    status: Optional[NotificationStatus] = None
    try_count: Optional[int] = None
    last_try_time: Optional[datetime] = None
    sent_time: Optional[datetime] = None
    fail_reason: Optional[str] = None


class Notification(NotificationBase):
    """알림 응답 모델"""
    notification_id: int = Field(..., description="알림 ID")
    status: NotificationStatus = Field(..., description="알림 상태")
    try_count: int = Field(..., description="재시도 횟수")
    created_time: datetime = Field(..., description="생성 시간")
    last_try_time: Optional[datetime] = Field(None, description="마지막 시도 시간")
    sent_time: Optional[datetime] = Field(None, description="발송 시간")
    fail_reason: Optional[str] = Field(None, description="실패 사유")
    
    @field_serializer('created_time')
    def serialize_created_time(self, value: datetime) -> str:
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
    
    @field_serializer('last_try_time')
    def serialize_last_try_time(self, value: Optional[datetime]) -> Optional[str]:
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
    
    @field_serializer('sent_time')
    def serialize_sent_time(self, value: Optional[datetime]) -> Optional[str]:
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
