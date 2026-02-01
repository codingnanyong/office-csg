"""Pydantic response models for Productivity OP Group endpoints."""

from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import ProductivityBaseFields


class ProductivityOpGroupRecord(ProductivityBaseFields):
    """Productivity OP Group Record"""
    op_group: Optional[str] = Field(None, description="Operation Group")
    
    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class ProductivityOpGroupResponse(BaseModel):
    """Productivity OP Group Response"""
    data: List[ProductivityOpGroupRecord] = Field(..., description="Productivity OP Group Data List")
    total: int = Field(..., description="Total Record Count")
    
    model_config = ConfigDict(from_attributes=True)

