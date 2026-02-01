"""Endpoints for querying Spare Part Inventory data."""
from fastapi import APIRouter, HTTPException, Query

from app.schemas.spare_part_inventory import (
    SparePartInventoryItem,
    SparePartInventoryResponse,
)
from app.services.spare_part_inventory import fetch_spare_part_inventory

router = APIRouter(prefix="/spare-part-inventory", tags=["spare-part-inventory"])


@router.get("", response_model=SparePartInventoryResponse, summary="Get Spare Part Inventory data with filters")
async def get_spare_part_inventory(
    mach_group: str | None = Query(None, description="Machine Group Filtering (Optional, if not provided, all machine groups)"),
    part_cd: str | None = Query(None, description="Part Code Filtering (Optional, if not provided, all part codes)"),
    part_nm: str | None = Query(None, description="Part Name Filtering (Optional, if not provided, all part names)"),
) -> SparePartInventoryResponse:
    """Retrieve Spare Part Inventory data with filters.
    
    Filter Conditions (All Optional):
    - mach_group: Machine Group Filtering (Optional, if not provided, all machine groups)
    - part_cd: Part Code Filtering (Optional, if not provided, all part codes)
    - part_nm: Part Name Filtering (Optional, if not provided, all part names)
    
    Examples:
    - All: GET /spare-part-inventory
    - Machine Group-wise: GET /spare-part-inventory?mach_group=GROUP01
    - Part Code-wise: GET /spare-part-inventory?part_cd=PART001
    - Part Name Search: GET /spare-part-inventory?part_nm=BEARING
    - Combination: GET /spare-part-inventory?mach_group=GROUP01&part_cd=PART001
    """
    try:
        items = await fetch_spare_part_inventory(
            mach_group=mach_group,
            part_cd=part_cd,
            part_nm=part_nm,
        )
        
        if not items:
            raise HTTPException(status_code=404, detail="No data found for the provided filters")
        
        return SparePartInventoryResponse(
            items=[SparePartInventoryItem.model_validate(item) for item in items],
            count=len(items)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e
