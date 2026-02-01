"""Pydantic response models for IP Rollgap endpoints."""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field


class IpRollgapRecord(BaseModel):
    """IP Rollgap Record"""
    roll_name: Optional[str] = Field(None, description="Roll Name (Roll A, Roll B, Roll C, Roll D)")
    capture_dt: Optional[datetime] = Field(None, description="Capture Date")
    gap_left: Optional[float] = Field(None, description="Left Gap (2 decimal places)")
    gap_right: Optional[float] = Field(None, description="Right Gap (2 decimal places)")
    
    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class IpRollgapResponse(BaseModel):
    """IP Rollgap Response"""
    data: List[IpRollgapRecord] = Field(..., description="IP Rollgap Data List")
    total: int = Field(..., description="Total Record Count")
    
    model_config = ConfigDict(from_attributes=True)

