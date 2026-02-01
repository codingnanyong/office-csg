"""Endpoints for querying Downtime data."""
from fastapi import APIRouter, HTTPException, Query
from datetime import date, datetime, timedelta
from typing import Optional

from app.schemas.downtime import DowntimeResponse, DowntimeRecord
from app.schemas.downtime_line import DowntimeLineResponse
from app.schemas.downtime_machine import DowntimeMachineResponse
from app.services.downtime import fetch_downtime
from app.services.downtime_line import fetch_downtime_line
from app.services.downtime_machine import fetch_downtime_machine

router = APIRouter(prefix="/downtime", tags=["downtime"])


def parse_date(date_str: str) -> date:
    """Parse date string to date object (supports yyyyMMdd, yyyy-MM-dd, yyyyMM, yyyy-MM formats)"""
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
                    raise ValueError(f"Invalid date format: {date_str}. Supported formats: yyyy-MM-dd, yyyyMMdd, yyyy-MM, or yyyyMM")


def parse_simple_date(date_str: str) -> date:
    """Parse date string to date object (supports yyyyMMdd or yyyy-MM-dd formats)"""
    try:
        # Try yyyy-MM-dd format
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        try:
            # Try yyyyMMdd format
            return datetime.strptime(date_str, "%Y%m%d").date()
        except ValueError:
            raise ValueError(f"Invalid date format: {date_str}. Supported formats: yyyy-MM-dd or yyyyMMdd")


@router.get("", response_model=DowntimeResponse, summary="Downtime Data Query")
async def get_downtime(
    date: Optional[str] = Query(None, description="Date Filtering (yyyyMMdd, yyyy-MM-dd, yyyyMM, yyyy-MM formats, default: current month)"),
    process: Optional[str] = Query(None, description="Process Code Filtering (os, ip, ph)"),
) -> DowntimeResponse:
    """Retrieve downtime data from the V_DOWNTIME view.
    
    Filter Conditions (All Optional):
    - date: Filter by specific date/month (yyyyMMdd, yyyy-MM-dd, yyyyMM, yyyy-MM formats, default: current month)
    - process: Filter by process code (os, ip, ph)
    
    By default, only the data for the current month is retrieved. If date is specified, the data for the month it belongs to is retrieved.
    
    Returns:
        List of downtime data and total record count
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
        
        records = await fetch_downtime(
            factory=None,
            building=None,
            line=None,
            date=parsed_date,
            process=process,
        )
        
        return DowntimeResponse(
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


@router.get("/line", response_model=DowntimeLineResponse, summary="Downtime Line Data Query")
async def get_downtime_line(
    line_cd: Optional[str] = Query(None, description="Line Code Filtering (Optional, if not provided, all lines)"),
    date: Optional[str] = Query(None, description="Business Date Filtering (yyyyMMdd or yyyy-MM-dd formats, default: yesterday)"),
) -> DowntimeLineResponse:
    """Retrieve downtime line data from the mart_downtime_by_line table.
    
    Filter Conditions (All Optional):
    - line_cd: Filter by line code (Optional, if not provided, all lines)
    - date: Filter by business date (yyyyMMdd or yyyy-MM-dd formats, default: yesterday)
    
    Returns:
        List of downtime line data and total record count
    """
    try:
        # Parse date parameter (default: yesterday)
        if date:
            parsed_date = parse_simple_date(date)
        else:
            parsed_date = (datetime.now() - timedelta(days=1)).date()
        
        records = await fetch_downtime_line(
            line_cd=line_cd,
            date=parsed_date,
        )
        
        return DowntimeLineResponse(
            data=records,
            total=len(records),
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        ) from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        ) from e


@router.get("/machine", response_model=DowntimeMachineResponse, summary="Downtime Machine Data Query")
async def get_downtime_machine(
    machine_cd: Optional[str] = Query(None, description="Machine Code Filtering (Optional, if not provided, all machines)"),
    date: Optional[str] = Query(None, description="Business Date Filtering (yyyyMMdd or yyyy-MM-dd formats, default: yesterday)"),
) -> DowntimeMachineResponse:
    """Retrieve downtime machine data from the mart_downtime_by_machine table.
    
    Filter Conditions (All Optional):
    - machine_cd: Filter by machine code (Optional, if not provided, all machines)
    - date: Filter by business date (yyyyMMdd or yyyy-MM-dd formats, default: yesterday)
    
    Returns:
        List of downtime machine data and total record count
    """
    try:
        # Parse date parameter (default: yesterday)
        if date:
            parsed_date = parse_simple_date(date)
        else:
            parsed_date = (datetime.now() - timedelta(days=1)).date()
        
        records = await fetch_downtime_machine(
            machine_cd=machine_cd,
            date=parsed_date,
        )
        
        return DowntimeMachineResponse(
            data=records,
            total=len(records),
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        ) from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        ) from e

