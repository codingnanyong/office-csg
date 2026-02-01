"""Pydantic response models for Productivity IP Zone endpoints."""

from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import ProductivityBaseFields


class ProductivityIpZoneRecord(ProductivityBaseFields):
    """Productivity IP Zone Record"""
    zone_cd: Optional[str] = Field(None, description="Zone Code")
    
    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class ProductivityIpZoneResponse(BaseModel):
    """Productivity IP Zone Response"""
    data: List[ProductivityIpZoneRecord] = Field(..., description="Productivity IP Zone Data List")
    total: int = Field(..., description="Total Record Count")
    
    model_config = ConfigDict(from_attributes=True)

