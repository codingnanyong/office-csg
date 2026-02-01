"""Endpoints for querying MTTR/MTBF data."""
from fastapi import APIRouter, HTTPException, Query
from datetime import date, datetime
from typing import Optional

from app.schemas.mttr_mtbf import MttrMtbfResponse
from app.services.mttr_mtbf import fetch_mttr_mtbf

router = APIRouter(prefix="/mttr-mtbf", tags=["mttr-mtbf"])


def parse_date(date_str: str) -> date:
    """Parse date string to date object (yyyyMMdd, yyyy-MM-dd, yyyyMM, yyyy-MM format supported)"""
    try:
        # Try yyyy-MM-dd format
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        try:
            # Try yyyyMMdd format
            return datetime.strptime(date_str, "%Y%m%d").date()
        except ValueError:
            try:
                # Try yyyy-MM format (first day of the month)
                parsed = datetime.strptime(date_str, "%Y-%m")
                return parsed.replace(day=1).date()
            except ValueError:
                try:
                    # Try yyyyMM format (first day of the month)
                    parsed = datetime.strptime(date_str, "%Y%m")
                    return parsed.replace(day=1).date()
                except ValueError:
                    raise ValueError(f"Invalid date format: {date_str}. Use yyyy-MM-dd, yyyyMMdd, yyyy-MM, or yyyyMM format")


@router.get("", response_model=MttrMtbfResponse, summary="MTTR/MTBF Data Query")
async def get_mttr_mtbf(
    date: Optional[str] = Query(None, description="Date Filtering (yyyyMMdd, yyyy-MM-dd, yyyyMM, yyyy-MM format, default: current month)"),
    process: Optional[str] = Query(None, description="Process Code Filtering (os, ip, ph)"),
) -> MttrMtbfResponse:
    """Retrieve MTTR/MTBF data from the V_MTTR_MTBF view.
    
    Filter Conditions (All Optional):
    - date: Filter by specific date/month (yyyyMMdd, yyyy-MM-dd, yyyyMM, yyyy-MM format, default: current month)
    - process: Filter by process code (os, ip, ph)
    
    By default, only the data for the current month is retrieved. If date is specified, the data for the month it belongs to is retrieved.
    
    Examples:
    - All: GET /mttr-mtbf
    - Specific date: GET /mttr-mtbf?date=20250301
    - Specific date (yyyy-MM-dd): GET /mttr-mtbf?date=2025-03-01
    - Specific month (yyyyMM): GET /mttr-mtbf?date=202503
    - Specific month (yyyy-MM): GET /mttr-mtbf?date=2025-03
    - Process-wise (current month): GET /mttr-mtbf?process=os
    - Process-wise (IP): GET /mttr-mtbf?process=ip
    - Process-wise (Phylon): GET /mttr-mtbf?process=ph
    
    Returns:
        List of MTTR/MTBF data and total record count
    """
    try:
        # Validate process parameter
        if process:
            process_upper = process.upper()
            if process_upper not in ['OS', 'IP', 'PH']:
                raise HTTPException(
                    status_code=400,
                    detail="process must be one of: os, ip, ph"
                )
        
        # Parse date parameter
        parsed_date = None
        if date:
            parsed_date = parse_date(date)
        
        records = await fetch_mttr_mtbf(
            date=parsed_date,
            process=process,
        )
        
        return MttrMtbfResponse(
            data=records,
            total=len(records),
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        ) from e

