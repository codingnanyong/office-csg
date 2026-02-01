"""Pydantic response models for Productivity endpoints."""

from datetime import date
from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field


class ProductivityRecord(BaseModel):
    """Productivity Record"""
    business_date: Optional[date] = Field(None, description="Business Date")
    op_group: Optional[str] = Field(None, description="Operation Group")
    op_cd: Optional[str] = Field(None, description="Operation Code (Only when op_cd is queried)")
    plan_qty: Optional[float] = Field(None, description="Plan Quantity")
    prod_qty: Optional[float] = Field(None, description="Production Quantity")
    defect_qty: Optional[float] = Field(None, description="Defect Quantity")
    quality_rate: Optional[float] = Field(None, description="Quality Rate")
    
    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class ProductivityResponse(BaseModel):
    """Productivity Response"""
    data: List[ProductivityRecord] = Field(..., description="Productivity Data List")
    total: int = Field(..., description="Total Record Count")
    
    model_config = ConfigDict(from_attributes=True)

