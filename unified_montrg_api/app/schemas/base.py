"""Base Pydantic schemas for common fields across different record types."""

from datetime import date
from typing import Optional
from pydantic import BaseModel, Field


class LocationBaseFields(BaseModel):
    """Common Location Fields (Factory, Building, Line)"""
    factory: Optional[str] = Field(None, description="공장 코드")
    factory_nm: Optional[str] = Field(None, description="공장명")
    building: Optional[str] = Field(None, description="건물 코드")
    building_nm: Optional[str] = Field(None, description="건물명")
    line_cd: Optional[str] = Field(None, description="라인 코드")
    line_nm: Optional[str] = Field(None, description="라인명")


class ProductivityBaseFields(BaseModel):
    """Common Productivity Fields"""
    business_date: Optional[date] = Field(None, description="영업일자")
    plan_qty: Optional[float] = Field(None, description="계획 수량")
    prod_qty: Optional[float] = Field(None, description="생산/실적 수량")
    defect_qty: Optional[float] = Field(None, description="불량 수량")
    quality_rate: Optional[float] = Field(None, description="품질률")


class DowntimeBaseFields(BaseModel):
    """Common Downtime Fields"""
    business_date: Optional[date] = Field(None, description="영업일자")
    down_time_target: Optional[float] = Field(None, description="다운타임 목표값")
    down_time_value: Optional[float] = Field(None, description="다운타임 실제값")

