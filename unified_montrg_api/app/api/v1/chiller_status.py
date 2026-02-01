"""API endpoints for Chiller Status operations."""

from typing import Optional
from fastapi import APIRouter, HTTPException, Query

from app.schemas.chiller_status import (
    ChillerStatusResponse,
    ChillerRunningStatusResponse,
    ChillerAlarmStatusResponse,
    ChillerStatusHistoryResponse,
)
from app.services.chiller_status import (
    fetch_chiller_status,
    fetch_chiller_running_status,
    fetch_chiller_alarm_status,
    fetch_chiller_status_history,
    fetch_chiller_status_history_by_range,
    resolve_date_range,
)

router = APIRouter(prefix="/chiller-status", tags=["chiller-status"])


@router.get("", response_model=ChillerStatusResponse, summary="Chiller Status 최신 데이터 조회")
async def get_chiller_status() -> ChillerStatusResponse:
    """Retrieve the latest data for each device_id from the Status table."""
    try:
        records = await fetch_chiller_status()
        return ChillerStatusResponse(data=records, total=len(records))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e


@router.get("/running", response_model=ChillerRunningStatusResponse, summary="Chiller Running Status 조회")
async def get_chiller_running_status() -> ChillerRunningStatusResponse:
    """Retrieve the running status for each device_id in the last 5 minutes from the Status table."""
    try:
        records = await fetch_chiller_running_status()
        return ChillerRunningStatusResponse(data=records, total=len(records))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e


@router.get("/alarms", response_model=ChillerAlarmStatusResponse, summary="Chiller Alarm Status 조회")
async def get_chiller_alarm_status() -> ChillerAlarmStatusResponse:
    """Retrieve the alarm occurrence status for each alarm from the Status table."""
    try:
        records = await fetch_chiller_alarm_status()
        return ChillerAlarmStatusResponse(data=records, total=len(records))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e


@router.get("/history", response_model=ChillerStatusHistoryResponse, summary="Chiller Status History Query (Last N Hours)")
async def get_chiller_status_history(
    device_id: Optional[str] = Query(None, description="Device ID or Number (Optional, if not provided, all devices). Example: '04', '4', 'CKP004'"),
    hours: int = Query(6, ge=1, le=168, description="Time range to query (1~168 hours, default: 6 hours)"),
) -> ChillerStatusHistoryResponse:
    """Retrieve the history of each device in the Status table aggregated by 10 minutes."""
    try:
        records = await fetch_chiller_status_history(device_id=device_id, hours=hours)
        return ChillerStatusHistoryResponse(data=records, total=len(records))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e


@router.get("/history/range", response_model=ChillerStatusHistoryResponse, summary="Chiller Status History Query (Time Range)")
async def get_chiller_status_history_by_range(
    device_id: Optional[str] = Query(None, description="Device ID or Number (Optional, if not provided, all devices). Example: '04', '4', 'CKP004'"),
    start_dt: Optional[str] = Query(None, description="Start Date/Time. Supported Formats: yyyy, yyyyMM, yyyyMMdd, yyyyMMdd HH:MM, yyyyMMdd HH:MM:SS"),
    end_dt: Optional[str] = Query(None, description="End Date/Time. Supported Formats: yyyy, yyyyMM, yyyyMMdd, yyyyMMdd HH:MM, yyyyMMdd HH:MM:SS"),
) -> ChillerStatusHistoryResponse:
    """Retrieve the history of each device in the Status table aggregated by 10 minutes within the specified time range.
    
    Date Format Examples:
    - yyyy: "2024" (Full 2024)
    - yyyyMM: "202412" (Full 2024 December)
    - yyyyMMdd: "20241205" (Full 2024 December 5)
    - yyyyMMdd HH:MM: "20241205 14:30" (2024 December 5 at 14:30)
    - yyyyMMdd HH:MM:SS: "20241205 14:30:00" (2024 December 5 at 14:30:00)
    """
    try:
        if not start_dt and not end_dt:
            raise HTTPException(
                status_code=400,
                detail="start_dt or end_dt is required."
            )
        
        # Convert date string to datetime (includes format validation)
        start_datetime, end_datetime = resolve_date_range(start_dt, end_dt)
        
        # Validate if start_dt is earlier than end_dt
        if start_datetime and end_datetime and start_datetime >= end_datetime:
            raise HTTPException(
                status_code=400,
                detail=f"start_dt ({start_dt}) must be earlier than end_dt ({end_dt})"
            )
        
        records = await fetch_chiller_status_history_by_range(
            device_id=device_id,
            start_dt=start_datetime,
            end_dt=end_datetime,
        )
        return ChillerStatusHistoryResponse(data=records, total=len(records))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e

