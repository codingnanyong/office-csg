"""
Pydantic schemas for API requests and responses
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from decimal import Decimal
from datetime import datetime


class Location(BaseModel):
    """위치 정보 모델 (외부 서비스용)"""
    loc_id: str = Field(..., description="위치 ID")
    factory: Optional[str] = Field(None, description="공장")
    building: Optional[str] = Field(None, description="건물명")
    floor: Optional[int] = Field(None, description="층수")
    area: Optional[str] = Field(None, description="구역")
    sensor_id: str = Field(..., description="센서 ID")


class MetricData(BaseModel):
    """측정값 데이터 모델"""
    value: Optional[Decimal] = Field(None, description="측정값")
    status: Optional[str] = Field(None, description="상태")


class MetricsData(BaseModel):
    """측정값들 모델"""
    temperature: Optional[MetricData] = Field(None, description="온도")
    humidity: Optional[MetricData] = Field(None, description="습도")
    pcv_temperature: Optional[MetricData] = Field(None, description="PCV 온도")


class LocationInfo(BaseModel):
    """위치 정보 모델"""
    factory: Optional[str] = Field(None, description="공장")
    building: Optional[str] = Field(None, description="건물명")
    floor: Optional[int] = Field(None, description="층수")
    loc_id: str = Field(..., description="위치 ID")
    area: Optional[str] = Field(None, description="구역")


class MeasurementData(BaseModel):
    """측정 데이터 모델"""
    location: LocationInfo = Field(..., description="위치 정보")
    metrics: MetricsData = Field(..., description="측정값들")


class TemperatureCurrentData(BaseModel):
    """현재 온도 데이터 모델"""
    ymd: str = Field(..., description="년월일")
    hh: str = Field(..., description="시간")
    measurements: List[MeasurementData] = Field(..., description="측정 데이터 목록")


class Threshold(BaseModel):
    """임계치 모델"""
    threshold_id: int = Field(..., description="임계치 ID")
    threshold_type: str = Field(..., description="임계치 타입")
    level: str = Field(..., description="레벨")
    min_value: Optional[Decimal] = Field(None, description="최소값")
    max_value: Optional[Decimal] = Field(None, description="최대값")
    upd_dt: Optional[datetime] = Field(None, description="업데이트 시간")
