"""Pydantic schemas for Spare Part Inventory API responses."""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class SparePartInventoryItem(BaseModel):
    """Spare Part Inventory Data Item Schema"""

    mach_group: str = Field(..., description="Machine Group")
    part_cd: str = Field(..., description="Part Code")
    part_nm: str = Field(..., description="Part Name")
    stock: int = Field(..., description="Stock")
    mach_id: str = Field(..., description="Machine ID")
    last_rep_dt: Optional[str] = Field(None, description="Last Repair Date (YYYYMMDD)")
    baseline_stock: int = Field(..., description="Baseline Stock")
    exp_cycle: int = Field(..., description="Expected Cycle")
    exp_rep_dt: Optional[str] = Field(None, description="Expected Repair Date (YYYYMMDD)")
    exp_orde_dt: Optional[str] = Field(None, description="Expected Order Date (YYYYMMDD)")

    model_config = ConfigDict(from_attributes=True)


class SparePartInventoryResponse(BaseModel):
    """Spare Part Inventory API Response Schema"""

    items: List[SparePartInventoryItem] = Field(..., description="Spare Part Inventory Data List")
    count: int = Field(..., description="Total Record Count")
