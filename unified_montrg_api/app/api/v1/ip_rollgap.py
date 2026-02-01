"""Endpoints for querying IP Rollgap data."""
from fastapi import APIRouter, HTTPException

from app.schemas.ip_rollgap import IpRollgapResponse
from app.services.ip_rollgap import fetch_ip_rollgap

router = APIRouter(prefix="/ip-rollgap", tags=["ip-rollgap"])


@router.get("", response_model=IpRollgapResponse, summary="IP Rollgap Data Query")
async def get_ip_rollgap() -> IpRollgapResponse:
    """Retrieve the latest data from the rollgap sensors.
    
    Filter Conditions (All Optional):
    - sensor_id: Filter by sensor ID (Optional, if not provided, all sensors)
    - capture_dt: Filter by capture date (Optional, if not provided, all capture dates)
    - gap_left: Filter by gap left (Optional, if not provided, all gap lefts)
    - gap_right: Filter by gap right (Optional, if not provided, all gap rights)
    
    Returns:
        List of IP Rollgap data and total record count
    """
    try:
        records = await fetch_ip_rollgap()
        
        return IpRollgapResponse(
            data=records,
            total=len(records),
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        ) from e

