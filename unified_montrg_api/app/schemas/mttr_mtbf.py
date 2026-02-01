"""Pydantic response models for MTTR/MTBF endpoints."""

from datetime import date
from typing import Annotated, List, Optional
from pydantic import BaseModel, ConfigDict, Field


class MttrMtbfRecord(BaseModel):
    """MTTR/MTBF Record"""
    process: Optional[str] = Field(None, description="Process Code (OS, IP, PH)")
    date: Annotated[Optional[date], Field(default=None, description="Date")]
    mttr_target: Optional[float] = Field(None, description="MTTR Target")
    mttr: Optional[float] = Field(None, description="MTTR Actual")
    mtbf_target: Optional[float] = Field(None, description="MTBF Target")
    mtbf: Optional[float] = Field(None, description="MTBF Actual")
    
    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class MttrMtbfResponse(BaseModel):
    """MTTR/MTBF Response"""
    data: List[MttrMtbfRecord] = Field(..., description="MTTR/MTBF Data List")
    total: int = Field(..., description="Total Record Count")
    
    model_config = ConfigDict(from_attributes=True)

