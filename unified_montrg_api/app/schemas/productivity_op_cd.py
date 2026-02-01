"""Pydantic response models for Productivity OP CD endpoints."""

from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import ProductivityBaseFields


class ProductivityOpCdRecord(ProductivityBaseFields):
    """Productivity OP CD Record"""
    op_cd: Optional[str] = Field(None, description="Operation Code")
    op_group: Optional[str] = Field(None, description="Operation Group")
    
    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class ProductivityOpCdResponse(BaseModel):
    """Productivity OP CD Response"""
    data: List[ProductivityOpCdRecord] = Field(..., description="Productivity OP CD Data List")
    total: int = Field(..., description="Total Record Count")
    
    model_config = ConfigDict(from_attributes=True)
