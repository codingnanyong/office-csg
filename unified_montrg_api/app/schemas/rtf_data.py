"""Pydantic response models for RTF data endpoints."""

from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field


class HmiData(BaseModel):
    rxdate: Optional[str] = Field(None, description="Timestamp of the measurement (ISO format)")
    pvalue: Optional[Decimal] = Field(None, description="Measured Pvalue rounded to 3 decimal places")


class HmiDataResponse(BaseModel):
    PID: str = Field(..., description="Patient identifier with leading zeros preserved")
    values: List[HmiData] = Field(..., description="Ordered list of measurement values")


class HmiDataMultiResponse(BaseModel):
    """Response model for multiple PID queries."""
    data: List[HmiDataResponse] = Field(..., description="List of data for each PID")
    total: int = Field(..., description="Total number of PIDs with data")

