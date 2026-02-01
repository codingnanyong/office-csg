"""
Pydantic schemas for API requests and responses
"""
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class LocationBase(BaseModel):
    """위치 정보 기본 모델"""
    loc_id: str = Field(..., description="위치 ID")
    factory: Optional[str] = Field(None, description="공장")
    building: Optional[str] = Field(None, description="건물명")
    floor: Optional[int] = Field(None, description="층수")
    area: Optional[str] = Field(None, description="구역")


class Location(LocationBase):
    """센서 위치 정보 응답 모델"""
    
    model_config = {
        "from_attributes": True
    }
    
    sensor_id: str = Field(..., description="센서 ID")
