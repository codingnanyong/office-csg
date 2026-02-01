"""Pydantic schemas for IPI Quality Temperature API responses."""

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class IpiQualityTemperatureDetailItem(BaseModel):
    """IPI Quality Temperature Detail Data Item Schema"""

    temp_type: str = Field(..., description="Temperature Type (L or U)")
    measurement_time: datetime = Field(..., description="Measurement Time")
    temperature: Decimal = Field(..., description="Temperature Value")
    seq_no: Optional[int] = Field(None, description="Sequence Number")

    @field_validator('temperature', mode='before')
    @classmethod
    def round_decimal(cls, v):
        """Round Decimal value to 2 decimal places"""
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        try:
            decimal_value = Decimal(str(v))
            return decimal_value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        except (ValueError, TypeError):
            return v

    model_config = ConfigDict(from_attributes=True)


class IpiQualityTemperatureItem(BaseModel):
    """IPI Quality Temperature Data Item Schema"""

    osnd_id: int = Field(..., description="Defect ID")
    osnd_dt: datetime = Field(..., description="Defect Date")
    machine_cd: Optional[str] = Field(None, description="Machine Code")
    station: Optional[int] = Field(None, description="Station")
    station_rl: Optional[str] = Field(None, description="Station RL")
    mold_id: Optional[str] = Field(None, description="Mold ID")
    reason_cd: Optional[str] = Field(None, description="Reason Code")
    size_cd: Optional[str] = Field(None, description="Size Code")
    lr_cd: Optional[str] = Field(None, description="LR Code")
    osnd_bt_qty: Optional[int] = Field(None, description="Defect Batch Quantity")
    details: List[IpiQualityTemperatureDetailItem] = Field(default_factory=list, description="Temperature Detail Records")

    model_config = ConfigDict(from_attributes=True)


class IpiQualityTemperatureResponse(BaseModel):
    """IPI Quality Temperature API Response Schema"""

    items: List[IpiQualityTemperatureItem] = Field(..., description="IPI Quality Temperature Data List")
    count: int = Field(..., description="Total Record Count")
    note: str = Field(
        default="reason_cd <> 'good' condition is always applied.",
        description="Filter Condition Guide"
    )

