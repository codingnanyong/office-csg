"""
Pydantic schemas for API requests and responses
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class MetricValue(BaseModel):
    """메트릭 값 모델"""
    value: str = Field(..., description="메트릭 값")
    status: str = Field(..., description="상태 (normal, warning, critical)")

class HourlyData(BaseModel):
    """시간별 데이터 모델"""
    ymd: str = Field(..., description="날짜 (yyyyMMdd)")
    hour: str = Field(..., description="시간 (HH)")
    metrics: Dict[str, str] = Field(..., description="메트릭 데이터")


class LocationData(BaseModel):
    """위치 데이터 모델"""
    factory: str = Field(..., description="공장명")
    building: Optional[str] = Field(None, description="건물명")
    floor: Optional[int] = Field(None, description="층수")
    loc_id: Optional[str] = Field(None, description="위치 ID")
    area: Optional[str] = Field(None, description="구역")
    date: List[HourlyData] = Field(..., description="시간별 데이터 목록")


class TemperatureAggregationResponse(BaseModel):
    """체감 온도 집계 응답 모델"""
    locations: List[LocationData] = Field(..., description="위치별 집계 데이터 목록")


class AggregationRequest(BaseModel):
    """집계 요청 모델"""
    location_id: Optional[str] = Field(None, description="위치 ID")
    factory: Optional[str] = Field(None, description="공장명")
    building: Optional[str] = Field(None, description="건물명")
    floor: Optional[int] = Field(None, description="층수")
    area: Optional[str] = Field(None, description="구역")
    start_date: str = Field(..., description="시작 날짜 (yyyyMMdd)")
    end_date: str = Field(..., description="종료 날짜 (yyyyMMdd)")
    start_hour: Optional[str] = Field("00", description="시작 시간 (HH)")
    end_hour: Optional[str] = Field("23", description="종료 시간 (HH)")
    metrics: List[str] = Field(default=["pcv_temperature_max", "pcv_temperature_avg", "temperature_max", "temperature_avg", "humidity_max", "humidity_avg"], description="집계할 메트릭 목록")


class AggregationStats(BaseModel):
    """집계 통계 모델"""
    total_count: int = Field(..., description="총 레코드 수")
    avg_value: Optional[float] = Field(None, description="평균값")
    min_value: Optional[float] = Field(None, description="최솟값")
    max_value: Optional[float] = Field(None, description="최댓값")
    std_dev: Optional[float] = Field(None, description="표준편차")
    percentiles: Optional[Dict[str, float]] = Field(None, description="백분위수")


class TimeSeriesData(BaseModel):
    """시계열 데이터 모델"""
    timestamp: datetime = Field(..., description="타임스탬프")
    value: float = Field(..., description="값")
    metadata: Optional[Dict[str, Any]] = Field(None, description="메타데이터")
