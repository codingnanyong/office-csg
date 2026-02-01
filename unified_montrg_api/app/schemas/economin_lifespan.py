"""Pydantic schemas for Economin Lifespan API responses."""

from decimal import Decimal
from typing import List

from pydantic import BaseModel, ConfigDict, Field


class EconominLifespanItem(BaseModel):
    """Economin Lifespan Data Item Schema"""

    machine_group: str = Field(..., description="Machine Group")
    use_year: int = Field(..., description="Use Year")
    maint_cost_y: Decimal = Field(..., description="Annual Maintenance Cost")
    depreciation_expense: Decimal = Field(..., description="Depreciation Expense")
    total_annual_cost: Decimal = Field(..., description="Total Annual Cost")

    model_config = ConfigDict(from_attributes=True)


class EconominLifespanResponse(BaseModel):
    """Economin Lifespan API Response Schema"""

    items: List[EconominLifespanItem] = Field(..., description="Economin Lifespan Data List")
    count: int = Field(..., description="Total Record Count")

