"""Pydantic response models for Work Order analysis endpoints."""

from typing import Dict, Optional
from pydantic import BaseModel, ConfigDict, Field


class WoStatusCount(BaseModel):
    """Wo Status Count"""
    wo_status: Optional[str] = Field(None, description="Work Order Status")
    count: int = Field(..., description="Count")
    
    model_config = ConfigDict(from_attributes=True)


class MachineWoStatus(BaseModel):
    """Machine Wo Status Count"""
    wo_statuses: Dict[str, WoStatusCount] = Field(
        ..., description="Wo Status Count Dictionary (key: wo_status)"
    )
    
    model_config = ConfigDict(from_attributes=True)


class LineWoStatus(BaseModel):
    """Line Wo Status Count (includes machine grouping)"""
    machines: Dict[str, MachineWoStatus] = Field(
        default_factory=dict, description="Machine Information Dictionary (key: machine_cd) (group_by=machine)"
    )
    wo_statuses: Dict[str, WoStatusCount] = Field(
        default_factory=dict, description="Wo Status Count Dictionary (key: wo_status) (group_by=op_line)"
    )
    
    model_config = ConfigDict(from_attributes=True)


class OperationWoStatus(BaseModel):
    """Operation Wo Status Count"""
    lines: Dict[int, LineWoStatus] = Field(
        default_factory=dict, description="Line Information Dictionary (key: line_no) (group_by=machine, op_line)"
    )
    wo_statuses: Dict[str, WoStatusCount] = Field(
        default_factory=dict, description="Wo Status Count Dictionary (key: wo_status) (group_by=op)"
    )
    
    model_config = ConfigDict(from_attributes=True)


class ProcessWoStatus(BaseModel):
    """Process Wo Status Count"""
    operations: Dict[str, OperationWoStatus] = Field(
        default_factory=dict, description="Operation Information Dictionary (key: op_cd) (group_by=machine, op_line, op)"
    )
    wo_statuses: Dict[str, WoStatusCount] = Field(
        default_factory=dict, description="Wo Status Count Dictionary (key: wo_status) (group_by=process)"
    )
    
    model_config = ConfigDict(from_attributes=True)


class WorkOrderAnalysisResponse(BaseModel):
    """Work Order Analysis Response (Hierarchical Structure: process_name -> op_cd -> line_no -> machine_cd -> wo_status)"""
    processes: Dict[str, ProcessWoStatus] = Field(
        ..., description="Process Information Dictionary (key: process_name)"
    )
    
    model_config = ConfigDict(from_attributes=True)
