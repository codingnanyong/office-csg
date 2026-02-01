"""Pydantic schemas for MC Master API responses."""

from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class Machine(BaseModel):
    """Machine Information Schema"""

    machine_no: int = Field(..., description="Machine Number")

    model_config = ConfigDict(from_attributes=True)


class Line(BaseModel):
    """Line Information Schema"""

    machines: Dict[str, Machine] = Field(
        ..., description="Machine Code to Machine Information Dictionary"
    )

    model_config = ConfigDict(from_attributes=True)


class Operation(BaseModel):
    """Operation Information Schema"""

    lines: Dict[int, Line] = Field(
        ..., description="Line Number to Line Information Dictionary"
    )

    model_config = ConfigDict(from_attributes=True)


class Process(BaseModel):
    """Process Information Schema"""

    operations: Dict[str, Operation] = Field(
        ..., description="Operation Code to Operation Information Dictionary"
    )

    model_config = ConfigDict(from_attributes=True)


class Plant(BaseModel):
    """Plant Information Schema"""

    processes: Dict[str, Process] = Field(
        ..., description="Process Name to Process Information Dictionary"
    )

    model_config = ConfigDict(from_attributes=True)


class Company(BaseModel):
    """Company Information Schema"""

    plants: Dict[str, Plant] = Field(
        ..., description="Plant Code to Plant Information Dictionary"
    )

    model_config = ConfigDict(from_attributes=True)


class Machines(BaseModel):
    """MC Master API Response Schema (Hierarchical Structure)"""

    companies: Dict[str, Company] = Field(
        ..., description="Company Name to Company Information Dictionary"
    )

    model_config = ConfigDict(from_attributes=True)
