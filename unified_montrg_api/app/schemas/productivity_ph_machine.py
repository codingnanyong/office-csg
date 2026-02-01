"""Pydantic response models for Productivity PH Machine endpoints."""

from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import ProductivityBaseFields


class ProductivityPhMachineRecord(ProductivityBaseFields):
    """Productivity PH Machine Record"""
    machine_cd: Optional[str] = Field(None, description="Machine Code")
    
    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class ProductivityPhMachineResponse(BaseModel):
    """Productivity PH Machine Response"""
    data: List[ProductivityPhMachineRecord] = Field(..., description="Productivity PH Machine Data List")
    total: int = Field(..., description="Total Record Count")
    
    model_config = ConfigDict(from_attributes=True)

