"""Pydantic response models for Chiller Status endpoints."""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, ConfigDict, Field


class ChillerStatusRecord(BaseModel):
    """Chiller Status Record"""
    chiller_name: Optional[str] = Field(None, description="Chiller Name (Chiller XX format)")
    upd_dt: Optional[datetime] = Field(None, description="Update Time")
    water_in_temp: Optional[float] = Field(None, description="Water In Temperature (1 decimal place)")
    water_out_temp: Optional[float] = Field(None, description="Water Out Temperature (1 decimal place)")
    external_temp: Optional[float] = Field(None, description="External Temperature (1 decimal place)")
    discharge_temp_1: Optional[float] = Field(None, description="Discharge Temperature 1 (1 decimal place)")
    discharge_temp_2: Optional[float] = Field(None, description="Discharge Temperature 2 (1 decimal place)")
    discharge_temp_3: Optional[float] = Field(None, description="Discharge Temperature 3 (1 decimal place)")
    discharge_temp_4: Optional[float] = Field(None, description="Discharge Temperature 4 (1 decimal place)")

    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class ChillerStatusResponse(BaseModel):
    """Chiller Status Response"""
    data: List[ChillerStatusRecord] = Field(..., description="Chiller Status Data List")
    total: int = Field(..., description="Total Record Count")

    model_config = ConfigDict(from_attributes=True)


class ChillerRunningStatusRecord(BaseModel):
    """Chiller Running Status Record"""
    device_id: Optional[str] = Field(None, description="Device ID")
    running: Optional[str] = Field(None, description="Running State (26th character of digitals field, recent 5 minutes data)")

    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class ChillerRunningStatusResponse(BaseModel):
    """Chiller Running Status Response"""
    data: List[ChillerRunningStatusRecord] = Field(..., description="Chiller Running Status Data List")
    total: int = Field(..., description="Total Record Count")

    model_config = ConfigDict(from_attributes=True)


class ChillerAlarmStatusRecord(BaseModel):
    """Chiller Alarm Status Record"""
    alarm_name: Optional[str] = Field(None, description="Alarm Name")
    alarm_count: Optional[int] = Field(None, description="Number of Devices with Alarm")
    device_info: Optional[List[Dict[str, Any]]] = Field(None, description="List of Device Information (device_id, device_name, upd_dt)")

    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class ChillerAlarmStatusResponse(BaseModel):
    """Chiller Alarm Status Response"""
    data: List[ChillerAlarmStatusRecord] = Field(..., description="Chiller Alarm Status Data List")
    total: int = Field(..., description="Total Record Count")

    model_config = ConfigDict(from_attributes=True)


class ChillerStatusHistoryItem(BaseModel):
    """Chiller Status History Item (10 minutes bucket aggregation)"""
    bucket_time: Optional[datetime] = Field(None, description="10 minutes bucket time")
    water_in_temp: Optional[float] = Field(None, description="Average Water In Temperature (2 decimal places)")
    water_out_temp: Optional[float] = Field(None, description="Average Water Out Temperature (2 decimal places)")
    sv_temp: Optional[float] = Field(None, description="Average SV Temperature (2 decimal places)")
    discharge_temp_1: Optional[float] = Field(None, description="Average Discharge Temperature 1 (2 decimal places)")
    discharge_temp_2: Optional[float] = Field(None, description="Average Discharge Temperature 2 (2 decimal places)")
    discharge_temp_3: Optional[float] = Field(None, description="Average Discharge Temperature 3 (2 decimal places)")
    discharge_temp_4: Optional[float] = Field(None, description="Average Discharge Temperature 4 (2 decimal places)")

    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class ChillerStatusHistoryRecord(BaseModel):
    """Chiller Status History Record (grouped by device_id)"""
    device_id: Optional[str] = Field(None, description="Device ID")
    history: List[ChillerStatusHistoryItem] = Field(default_factory=list, description="10 minutes bucket history data list")

    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
    )


class ChillerStatusHistoryResponse(BaseModel):
    """Chiller Status History Response"""
    data: List[ChillerStatusHistoryRecord] = Field(..., description="Chiller Status History Data List")
    total: int = Field(..., description="Total Record Count")

    model_config = ConfigDict(from_attributes=True)

