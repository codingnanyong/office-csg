"""
Pydantic schemas for API requests and responses
"""
from pydantic import BaseModel, Field, field_validator, field_serializer
from typing import Optional
from enum import Enum
from decimal import Decimal
from datetime import datetime, timezone, timedelta


class AlertType(str, Enum):
    """알람 타입 열거형"""
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"
    PCV_TEMPERATURE = "pcv_temperature"
    ENV_TEMP = "env_temp"
    ENV_HUMIDITY = "env_humidity"
    PCV_TEMP = "pcv_temp"


class AlertLevel(str, Enum):
    """알람 레벨 열거형"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"
    ORANGE = "orange"
    WARNING = "warning"
    INFO = "info"


class AlertBase(BaseModel):
    """알람 기본 모델"""
    loc_id: Optional[str] = Field(None, description="위치 ID")
    sensor_id: str = Field(..., description="센서 ID")
    alert_type: AlertType = Field(..., description="알람 타입")
    alert_level: AlertLevel = Field(..., description="알람 레벨")
    threshold_id: int = Field(..., description="임계치 ID")
    threshold_map_id: Optional[int] = Field(None, description="임계치 매핑 ID")
    threshold_type: str = Field(..., description="임계치 타입")
    threshold_level: str = Field(..., description="임계치 레벨")
    measured_value: Optional[Decimal] = Field(None, description="측정값")
    threshold_min: Optional[Decimal] = Field(None, description="임계치 최소값")
    threshold_max: Optional[Decimal] = Field(None, description="임계치 최대값")
    message: Optional[str] = Field(None, description="알람 메시지")


class AlertCreate(AlertBase):
    """알람 생성 모델"""
    alert_time: Optional[datetime] = Field(None, description="알람 발생 시간 (기본값: 현재 시간)")


class Alert(AlertBase):
    """알람 응답 모델"""
    alert_id: int = Field(..., description="알람 ID")
    alert_time: datetime = Field(..., description="알람 발생 시간")
    
    @field_serializer('alert_time')
    def serialize_alert_time(self, value: datetime) -> str:
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