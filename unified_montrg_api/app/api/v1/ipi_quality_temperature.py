"""Endpoints for querying IPI Quality Temperature data."""
from fastapi import APIRouter, HTTPException, Query

from app.schemas.ipi_quality_temperature import (
    IpiQualityTemperatureItem,
    IpiQualityTemperatureResponse,
    IpiQualityTemperatureDetailItem,
)
from app.services.ipi_quality_temperature import fetch_ipi_quality_temperature

router = APIRouter(prefix="/ipi-quality-temperature", tags=["ipi-quality-temperature"])


@router.get("", response_model=IpiQualityTemperatureResponse, summary="Get IPI Quality Temperature data by osnd_id")
async def get_ipi_quality_temperature(
    osnd_id: int = Query(..., description="Defect ID (Required)"),
) -> IpiQualityTemperatureResponse:
    """Retrieve IPI Quality Temperature data by osnd_id.
    
    Important: 
    - `reason_cd <> 'good'` condition is always applied.
    - Only records with both L (Lower) and U (Upper) temperature detail records are returned.
    - Each item includes a list of temperature detail records in the `details` field.

    Args:
        osnd_id: Defect ID (Required)

    Returns:
        List of IPI Quality Temperature data with detail records and total record count
    """
    try:
        items = await fetch_ipi_quality_temperature(osnd_id=osnd_id)
        
        if not items:
            raise HTTPException(status_code=404, detail="No data found for the provided filters")
        
        # Convert to schema objects
        response_items = []
        for item in items:
            # Convert detail records
            details = [
                IpiQualityTemperatureDetailItem.model_validate(detail)
                for detail in item.get("details", [])
            ]
            
            # Create main item with details
            main_item = IpiQualityTemperatureItem(
                osnd_id=item["osnd_id"],
                osnd_dt=item["osnd_dt"],
                machine_cd=item.get("machine_cd"),
                station=item.get("station"),
                station_rl=item.get("station_rl"),
                mold_id=item.get("mold_id"),
                reason_cd=item.get("reason_cd"),
                size_cd=item.get("size_cd"),
                lr_cd=item.get("lr_cd"),
                osnd_bt_qty=item.get("osnd_bt_qty"),
                details=details,
            )
            response_items.append(main_item)
        
        return IpiQualityTemperatureResponse(
            items=response_items,
            count=len(response_items)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e

