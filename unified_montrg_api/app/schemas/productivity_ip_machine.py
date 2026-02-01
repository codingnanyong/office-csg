"""Pydantic response models for Productivity IP Machine endpoints."""

from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import ProductivityBaseFields


class ProductivityIpMachineRecord(ProductivityBaseFields):
    """Productivity IP Machine Record"""
    machine_cd: Optional[str] = Field(None, description="Machine Code")
    
    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class ProductivityIpMachineResponse(BaseModel):
    """Productivity IP Machine Response"""
    data: List[ProductivityIpMachineRecord] = Field(..., description="Productivity IP Machine Data List")
    total: int = Field(..., description="Total Record Count")
    
    model_config = ConfigDict(from_attributes=True)

