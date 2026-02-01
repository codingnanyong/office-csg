"""
Pydantic schemas for API requests and responses
"""
from pydantic import BaseModel, Field, field_validator
from typing import Optional
from enum import Enum
from decimal import Decimal
from datetime import datetime


class ThresholdType(str, Enum):
    """임계치 타입 열거형"""
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"
    PCV_TEMPERATURE = "pcv_temperature"


class Level(str, Enum):
    """레벨 열거형"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"
    ORANGE = "orange"


class ThresholdBase(BaseModel):
    """임계치 기본 모델"""
    threshold_type: ThresholdType = Field(..., description="임계치 타입")
    level: Level = Field(..., description="레벨")
    min_value: Optional[Decimal] = Field(None, description="최소값")
    max_value: Optional[Decimal] = Field(None, description="최대값")

    @field_validator('max_value')
    @classmethod
    def max_value_must_be_greater_than_min(cls, v, info):
        """최대값은 최소값보다 커야 함"""
        if info.data and 'min_value' in info.data and v is not None and info.data['min_value'] is not None:
            if v <= info.data['min_value']:
                raise ValueError('max_value must be greater than min_value')
        return v


class ThresholdCreate(ThresholdBase):
    """임계치 생성 모델"""
    pass


class ThresholdUpdate(BaseModel):
    """임계치 수정 모델"""
    threshold_type: Optional[ThresholdType] = Field(None, description="임계치 타입")
    level: Optional[Level] = Field(None, description="레벨")
    min_value: Optional[Decimal] = Field(None, description="최소값")
    max_value: Optional[Decimal] = Field(None, description="최대값")

    @field_validator('max_value')
    @classmethod
    def max_value_must_be_greater_than_min(cls, v, info):
        """최대값은 최소값보다 커야 함"""
        if info.data and 'min_value' in info.data and v is not None and info.data['min_value'] is not None:
            if v <= info.data['min_value']:
                raise ValueError('max_value must be greater than min_value')
        return v


class Threshold(ThresholdBase):
    """임계치 응답 모델"""
    threshold_id: int = Field(..., description="임계치 ID")
    upd_dt: Optional[datetime] = Field(None, description="업데이트 시간")
    
    model_config = {
        "from_attributes": True
    }
