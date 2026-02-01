"""Endpoints for querying CTM HMI data."""
from typing import List
from fastapi import APIRouter, HTTPException, Query

from app.core.database import available_databases
from app.schemas.rtf_data import HmiDataResponse, HmiDataMultiResponse
from app.services.ctm_data import (
    fetch_ctm_data_by_pid,
    fetch_ctm_data_by_pid_and_date_key,
    fetch_ctm_data_by_pid_and_range,
    fetch_ctm_data_latest,
    fetch_ctm_data_latest_by_pids,
)

router = APIRouter(prefix="/ctm-data", tags=["ctm-data"])

# Available Databases for ctm-data API
# Only CTM HMI MySQL is used: ctm01, ctm02, etc. All CTM numbers
ALL_DATABASES = available_databases()
AVAILABLE_DATABASES = [db for db in ALL_DATABASES if db.startswith("ctm")]


@router.get("", response_model=HmiDataMultiResponse, summary="Fetch CTM data by multiple PIDs")
async def get_ctm_data_by_pids(
    pid: List[int] = Query(..., description="List of Patient IDs to query", min_length=1),
    limit: int = Query(500, ge=1, le=5000, description="Maximum number of rows to return per PID"),
    db: str | None = Query(
        None,
        description="Database alias to query. Leave empty for default.",
        pattern="^[A-Za-z0-9_\\-]+$",
    ),
) -> HmiDataMultiResponse:
    """Retrieve CTM data filtered by multiple PIDs.
    
    Filter Conditions (All Optional):
    - pid: Filter by patient ID (Required, at least one PID)
    - limit: Filter by maximum number of rows to return per PID (1~5000, default: 500)
    - db: Filter by database alias (Optional, if not provided, default database is used)
    
    Returns:
        List of CTM data and total record count
    """
    if db and db not in AVAILABLE_DATABASES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown database alias '{db}'. Available: {AVAILABLE_DATABASES}",
        )

    results = []
    for p in pid:
        response = await fetch_ctm_data_by_pid(pid=p, limit=limit, db_alias=db)
        if response:
            results.append(response)

    return HmiDataMultiResponse(data=results, total=len(results))


@router.get("/latest", response_model=HmiDataMultiResponse, summary="Fetch the most recent CTM data by multiple PIDs")
async def get_ctm_data_latest_by_pids(
    pid: List[int] = Query(..., description="List of Patient IDs to query", min_length=1),
    db: str | None = Query(
        None,
        description="Database alias to query. Leave empty for default.",
        pattern="^[A-Za-z0-9_\\-]+$",
    ),
) -> HmiDataMultiResponse:
    """Retrieve the most recent CTM data for multiple PIDs.
    
    Filter Conditions (All Optional):
    - pid: Filter by patient ID (Required, at least one PID)
    - db: Filter by database alias (Optional, if not provided, default database is used)
    
    Returns:
        List of CTM data and total record count
    """
    if db and db not in AVAILABLE_DATABASES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown database alias '{db}'. Available: {AVAILABLE_DATABASES}",
        )

    results = await fetch_ctm_data_latest_by_pids(pids=pid, db_alias=db)
    return HmiDataMultiResponse(data=results, total=len(results))


@router.get("/{pid}", response_model=HmiDataResponse, summary="Fetch CTM data by single PID")
async def get_ctm_data_by_pid(
    pid: int,
    limit: int = Query(500, ge=1, le=5000, description="Maximum number of rows to return"),
    db: str | None = Query(
        None,
        description="Database alias to query. Leave empty for default.",
        pattern="^[A-Za-z0-9_\\-]+$",
    ),
) -> HmiDataResponse:
    """Retrieve CTM data filtered by PID.
    
    Filter Conditions (All Optional):
    - pid: Filter by patient ID (Required, at least one PID)
    - limit: Filter by maximum number of rows to return per PID (1~5000, default: 500)
    - db: Filter by database alias (Optional, if not provided, default database is used)
    
    Returns:
        List of CTM data and total record count
    """
    if db and db not in AVAILABLE_DATABASES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown database alias '{db}'. Available: {AVAILABLE_DATABASES}",
        )

    response = await fetch_ctm_data_by_pid(pid=pid, limit=limit, db_alias=db)

    if response is None:
        raise HTTPException(status_code=404, detail="No data found for the provided PID")

    return response


@router.get(
    "/{pid}/dates/{date_key}",
    response_model=HmiDataResponse,
    summary="Fetch CTM data by PID and date key (YYYY, YYYYMM, YYYYMMDD)",
)
async def get_ctm_data_by_date_key(
    pid: int,
    date_key: str,
    limit: int = Query(500, ge=1, le=5000, description="Maximum number of rows to return"),
    db: str | None = Query(
        None,
        description="Database alias to query. Leave empty for default.",
        pattern="^[A-Za-z0-9_\\-]+$",
    ),
) -> HmiDataResponse:
    """Retrieve CTM data filtered by PID and a specific date key.
    
    Filter Conditions (All Optional):
    - pid: Filter by patient ID (Required, at least one PID)
    - limit: Filter by maximum number of rows to return per PID (1~5000, default: 500)
    - db: Filter by database alias (Optional, if not provided, default database is used)
    
    Returns:
        List of CTM data and total record count
    """
    if db and db not in AVAILABLE_DATABASES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown database alias '{db}'. Available: {AVAILABLE_DATABASES}",
        )

    try:
        response = await fetch_ctm_data_by_pid_and_date_key(
            pid=pid,
            limit=limit,
            date_key=date_key,
            db_alias=db,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if response is None:
        raise HTTPException(status_code=404, detail="No data found for the provided PID/date key")

    return response


@router.get(
    "/{pid}/range",
    response_model=HmiDataResponse,
    summary="Fetch CTM data by PID within a datetime range",
)
async def get_ctm_data_by_range(
    pid: int,
    start_key: str | None = Query(
        None, description="Start date key (YYYY, YYYYMM, or YYYYMMDD inclusive)"
    ),
    end_key: str | None = Query(
        None, description="End date key (YYYY, YYYYMM, or YYYYMMDD exclusive)"
    ),
    limit: int = Query(500, ge=1, le=5000, description="Maximum number of rows to return"),
    db: str | None = Query(
        None,
        description="Database alias to query. Leave empty for default.",
        pattern="^[A-Za-z0-9_\\-]+$",
    ),
) -> HmiDataResponse:
    """Retrieve CTM data filtered by PID and date-key range.
    
    Filter Conditions (All Optional):
    - pid: Filter by patient ID (Required, at least one PID)
    - limit: Filter by maximum number of rows to return per PID (1~5000, default: 500)
    - db: Filter by database alias (Optional, if not provided, default database is used)
    
    Returns:
        List of CTM data and total record count
    """
    if db and db not in AVAILABLE_DATABASES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown database alias '{db}'. Available: {AVAILABLE_DATABASES}",
        )

    if not start_key and not end_key:
        raise HTTPException(status_code=400, detail="start_key or end_key must be provided.")

    try:
        response = await fetch_ctm_data_by_pid_and_range(
            pid=pid,
            limit=limit,
            start_key=start_key,
            end_key=end_key,
            db_alias=db,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if response is None:
        raise HTTPException(status_code=404, detail="No data found for the provided filters")

    return response

