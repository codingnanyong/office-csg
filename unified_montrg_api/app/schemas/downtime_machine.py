"""Pydantic response models for Downtime Machine endpoints."""

from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field

from app.schemas.base import DowntimeBaseFields, LocationBaseFields


class DowntimeMachineRecord(DowntimeBaseFields, LocationBaseFields):
    """Downtime Machine Record"""
    machine_cd: Optional[str] = Field(None, description="Machine Code")
    mes_machine_nm: Optional[str] = Field(None, description="MES Machine Name")
    
    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class DowntimeMachineResponse(BaseModel):
    """Downtime Machine Response"""
    data: List[DowntimeMachineRecord] = Field(..., description="Downtime Machine Data List")
    total: int = Field(..., description="Total Record Count")
    
    model_config = ConfigDict(from_attributes=True)

