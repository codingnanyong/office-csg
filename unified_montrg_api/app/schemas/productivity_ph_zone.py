"""Pydantic response models for Productivity PH Zone endpoints."""

from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import ProductivityBaseFields


class ProductivityPhZoneRecord(ProductivityBaseFields):
    """Productivity PH Zone Record"""
    line_group: Optional[str] = Field(None, description="Line Group")
    
    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class ProductivityPhZoneResponse(BaseModel):
    """Productivity PH Zone Response"""
    data: List[ProductivityPhZoneRecord] = Field(..., description="Productivity PH Zone Data List")
    total: int = Field(..., description="Total Record Count")
    
    model_config = ConfigDict(from_attributes=True)

