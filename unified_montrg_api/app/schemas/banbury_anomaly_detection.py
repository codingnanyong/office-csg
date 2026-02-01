"""Pydantic response models for Banbury Anomaly Detection endpoints."""

from datetime import datetime, timedelta, timezone
from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field, field_serializer

KST = timezone(timedelta(hours=9))


def _to_kst(dt: datetime) -> str:
    """Serialize datetime as KST ISO8601 string."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(KST).isoformat()


class BanburyAnomalyResultRecord(BaseModel):
    """Banbury Anomaly Result Record"""

    no: str = Field(..., description="Number")
    shift: Optional[int] = Field(None, description="Shift")
    cycle_start: datetime = Field(..., description="Cycle Start Time")
    cycle_end: datetime = Field(..., description="Cycle End Time")
    mode: Optional[str] = Field(None, description="Mode")
    mix_duration_sec: float = Field(..., description="Mix Duration (seconds)")
    max_temp: Optional[float] = Field(None, description="Max Temperature")
    is_3_stage: bool = Field(..., description="3 Stage")
    is_anomaly: bool = Field(..., description="Anomaly")
    anomaly_prob: float = Field(..., description="Anomaly Probability (percentage, 2 decimal places)")
    filtered_num: int = Field(..., description="Filtered Number")
    peak_count: int = Field(..., description="Peak Count")
    result: bool = Field(..., description="Result")

    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )

    @field_serializer("cycle_start", "cycle_end", when_used="json")
    def serialize_dt_as_kst(self, value: datetime) -> str:
        """Serialize datetime fields as KST ISO8601 strings (Pydantic v2 style)."""
        return _to_kst(value)


class BanburyAnomalyResultResponse(BaseModel):
    """Banbury Anomaly Result Response"""

    data: List[BanburyAnomalyResultRecord] = Field(..., description="Banbury Anomaly Result Data List")
    total: int = Field(..., description="Total Record Count")

    model_config = ConfigDict(from_attributes=True)

