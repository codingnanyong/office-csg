"""
Pydantic schemas for API requests and responses
"""
from pydantic import BaseModel, Field, field_serializer
from typing import Optional
from datetime import datetime, timezone, timedelta


class MappingBase(BaseModel):
    """매핑 기본 모델"""
    sensor_id: str = Field(..., description="센서 ID")
    threshold_id: int = Field(..., description="임계치 ID")
    duration_seconds: int = Field(60, description="지속 시간 (초 단위)")
    enabled: bool = Field(True, description="활성화 여부")
    effective_from: Optional[datetime] = Field(None, description="유효 시작 시간")
    effective_to: Optional[datetime] = Field(None, description="유효 종료 시간")


class MappingCreate(MappingBase):
    """매핑 생성 모델"""
    pass


class MappingUpdate(BaseModel):
    """매핑 수정 모델"""
    duration_seconds: Optional[int] = None
    enabled: Optional[bool] = None
    effective_from: Optional[datetime] = None
    effective_to: Optional[datetime] = None


class Mapping(MappingBase):
    """매핑 응답 모델"""
    map_id: int = Field(..., description="매핑 ID")
    upd_dt: Optional[datetime] = Field(None, description="수정 시간")
    
    @field_serializer('upd_dt', 'effective_from', 'effective_to')
    def serialize_datetime(self, value: Optional[datetime]) -> Optional[str]:
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
