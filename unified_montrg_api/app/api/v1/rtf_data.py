"""Endpoints for querying RTF data."""
from fastapi import APIRouter, HTTPException, Query

from app.core.database import available_databases
from app.schemas.rtf_data import HmiDataResponse
from app.services.rtf_data import (
    fetch_rtf_data_by_pid,
    fetch_rtf_data_by_pid_and_date_key,
    fetch_rtf_data_by_pid_and_range,
    fetch_rtf_data_latest,
)

router = APIRouter(prefix="/ip-data", tags=["ip-data"])

# ip-data API에서 사용 가능한 데이터베이스 (unified-montrg PostgreSQL 제외)
# MySQL만 사용: ip04, ip12, ip20, ip34, ip37 등 모든 IP 번호
ALL_DATABASES = available_databases()
AVAILABLE_DATABASES = [db for db in ALL_DATABASES if db != "montrg"]  # unified-montrg PostgreSQL 제외


@router.get("/{pid}", response_model=HmiDataResponse, summary="Fetch RTF data by PID")
async def get_rtf_data_by_pid(
    pid: int,
    limit: int = Query(500, ge=1, le=5000, description="Maximum number of rows to return"),
    db: str | None = Query(
        None,
        description="Database alias to query. Leave empty for default.",
        regex="^[A-Za-z0-9_\\-]+$",
    ),
) -> HmiDataResponse:
    """Retrieve RTF data filtered by PID."""
    if db and db not in AVAILABLE_DATABASES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown database alias '{db}'. Available: {AVAILABLE_DATABASES}",
        )

    response = await fetch_rtf_data_by_pid(pid=pid, limit=limit, db_alias=db)

    if response is None:
        raise HTTPException(status_code=404, detail="No data found for the provided PID")

    return response


@router.get(
    "/{pid}/dates/{date_key}",
    response_model=HmiDataResponse,
    summary="Fetch RTF data by PID and date key (YYYY, YYYYMM, YYYYMMDD)",
)
async def get_rtf_data_by_date_key(
    pid: int,
    date_key: str,
    limit: int = Query(500, ge=1, le=5000, description="Maximum number of rows to return"),
    db: str | None = Query(
        None,
        description="Database alias to query. Leave empty for default.",
        regex="^[A-Za-z0-9_\\-]+$",
    ),
) -> HmiDataResponse:
    """Retrieve RTF data filtered by PID and a specific date key."""
    if db and db not in AVAILABLE_DATABASES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown database alias '{db}'. Available: {AVAILABLE_DATABASES}",
        )

    try:
        response = await fetch_rtf_data_by_pid_and_date_key(
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
    summary="Fetch RTF data by PID within a datetime range",
)
async def get_rtf_data_by_range(
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
        regex="^[A-Za-z0-9_\\-]+$",
    ),
) -> HmiDataResponse:
    """Retrieve RTF data filtered by PID and date-key range."""
    if db and db not in AVAILABLE_DATABASES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown database alias '{db}'. Available: {AVAILABLE_DATABASES}",
        )

    if not start_key and not end_key:
        raise HTTPException(status_code=400, detail="start_key or end_key must be provided.")

    try:
        response = await fetch_rtf_data_by_pid_and_range(
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


@router.get(
    "/{pid}/latest",
    response_model=HmiDataResponse,
    summary="Fetch the most recent RTF data by PID",
)
async def get_rtf_data_latest(
    pid: int,
    db: str | None = Query(
        None,
        description="Database alias to query. Leave empty for default.",
        regex="^[A-Za-z0-9_\\-]+$",
    ),
) -> HmiDataResponse:
    """Retrieve the most recent RTF data for a given PID."""
    if db and db not in AVAILABLE_DATABASES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown database alias '{db}'. Available: {AVAILABLE_DATABASES}",
        )

    response = await fetch_rtf_data_latest(pid=pid, db_alias=db)

    if response is None:
        raise HTTPException(status_code=404, detail="No data found for the provided PID")

    return response

