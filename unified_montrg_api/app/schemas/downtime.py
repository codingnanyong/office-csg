"""Pydantic response models for Downtime endpoints."""

from datetime import date
from typing import Annotated, List, Optional
from pydantic import BaseModel, ConfigDict, Field


class DowntimeRecord(BaseModel):
    """Downtime Record"""
    factory: Optional[str] = Field(None, description="Factory Code")
    factory_nm: Optional[str] = Field(None, description="Factory Name")
    building: Optional[str] = Field(None, description="Building Code")
    building_nm: Optional[str] = Field(None, description="Building Name")
    line: Optional[str] = Field(None, description="Line Code")
    line_nm: Optional[str] = Field(None, description="Line Name")
    process: Optional[str] = Field(None, description="Process Code (OS, IP, PH)")
    date: Annotated[Optional[date], Field(default=None, description="Date")]
    down_time_target: Optional[float] = Field(None, description="Downtime Target")
    down_time_value: Optional[float] = Field(None, description="Downtime Value")
    
    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class DowntimeResponse(BaseModel):
    """Downtime Response"""
    data: List[DowntimeRecord] = Field(..., description="Downtime Data List")
    total: int = Field(..., description="Total Record Count")
    
    model_config = ConfigDict(from_attributes=True)

