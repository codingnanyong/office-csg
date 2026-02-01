"""Pydantic response models for Downtime Line endpoints."""

from typing import List
from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import DowntimeBaseFields, LocationBaseFields


class DowntimeLineRecord(DowntimeBaseFields, LocationBaseFields):
    """Downtime Line Record"""
    
    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class DowntimeLineResponse(BaseModel):
    """Downtime Line Response"""
    data: List[DowntimeLineRecord] = Field(..., description="Downtime Line Data List")
    total: int = Field(..., description="Total Record Count")
    
    model_config = ConfigDict(from_attributes=True)

