"""Pydantic schemas for Machine Grade API responses."""

from datetime import date
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class MachineGradeItem(BaseModel):
    """Machine Grade Data Item Schema"""

    mc_nm: str = Field(..., description="Machine Name")
    ym: date = Field(..., description="Year Month")
    failures_12m: int = Field(..., description="12 Months Failures Count")
    operation_time: int = Field(..., description="Operation Time")
    repair_time: int = Field(..., description="Repair Time")
    operation_time_day: Optional[Decimal] = Field(None, description="Operation Time per Day")
    mtbf_min: Optional[Decimal] = Field(None, description="MTBF (Minutes)")
    mttr_min: Optional[Decimal] = Field(None, description="MTTR (Minutes)")
    availability: Optional[Decimal] = Field(None, description="Availability")
    machine_group: Optional[str] = Field(None, description="Machine Group")
    mtbf_z: Optional[Decimal] = Field(None, description="MTBF Z-score")
    mttr_z: Optional[Decimal] = Field(None, description="MTTR Z-score")
    grade: Optional[str] = Field(None, description="Grade")
    comment: Optional[str] = Field(None, description="Comment")
    pm_cycle_week: Optional[int] = Field(None, description="PM Cycle (weeks)")

    model_config = ConfigDict(from_attributes=True)


class MachineGradeResponse(BaseModel):
    """Machine Grade API Response Schema"""

    items: List[MachineGradeItem] = Field(..., description="Machine Grade Data List")
    count: int = Field(..., description="Total Record Count")

