"""API endpoints for Banbury Anomaly Detection operations."""

from typing import Optional
from datetime import date
from fastapi import APIRouter, HTTPException, Query

from app.schemas.banbury_anomaly_detection import BanburyAnomalyResultResponse
from app.services.banbury_anomaly_detection import (
    fetch_banbury_anomaly_results,
    _resolve_cycle_start_range,
)

router = APIRouter(prefix="/banbury-anomaly-detection", tags=["banbury-anomaly-detection"])


@router.get("", response_model=BanburyAnomalyResultResponse, summary="Banbury Anomaly Result 조회")
async def get_banbury_anomaly_results(
    no: Optional[str] = Query(None, description="No Filtering"),
    shift: Optional[int] = Query(None, description="Shift Filtering (1, 2, 3)"),
    mode: Optional[str] = Query(None, description="Run Mode Filtering (예: normal)"),
    is_anomaly: Optional[bool] = Query(None, description="Anomaly = True (AI(CNN) Model Result) Filtering"),
    is_3_stage: Optional[bool] = Query(None, description="Rull Base Filtering"),
    result: Optional[bool] = Query(None, description="Result Filtering (Rule Base = True and Anomaly = True (AI(CNN) Model Result) Then Result = True Else False)"),
    cycle_start_from: Optional[str] = Query(
        None,
        description="Cycle Start Time Start Range. Supported Formats: yyyy, yyyyMM, yyyyMMdd, yyyy-MM, YYYY-MM-DD, YYYY-MM-DD HH:MM, YYYY-MM-DD HH:MM:SS",
    ),
    cycle_start_to: Optional[str] = Query(
        None,
        description="Cycle Start Time End Range. Supported Formats: yyyy, yyyyMM, yyyyMMdd, yyyy-MM, YYYY-MM-DD, YYYY-MM-DD HH:MM, YYYY-MM-DD HH:MM:SS",
    ),
) -> BanburyAnomalyResultResponse:
    """Banbury Anomaly Result Data Query.

    **Banbury Anomaly Result Data Query Default Behavior**: 
    - If no date parameters are provided and no other filter conditions are provided, the default value is today's date.
    - Date parameters and other filter conditions can be used together.

    **Supported Date Formats**:

    - `yyyy`: "2024" → Full 2024 year
    - `yyyyMM`: "202412" → Full 2024 December
    - `yyyy-MM`: "2024-12" → Full 2024 December
    - `yyyyMMdd`: "20241205" → Full 2024 December 5th
    - `YYYY-MM-DD`: "2024-12-05" → Full 2024 December 5th
    - `YYYY-MM-DD HH:MM:SS`: "2024-12-05 14:30:00" → Exact time
    - `YYYY-MM-DD HH:MM`: "2024-12-05 14:30" → Exact time

    # Date Range Query (Year)
    GET /banbury-anomaly-detection?cycle_start_from=2024&cycle_start_to=2024

    # Date Range Query (Month)
    GET /banbury-anomaly-detection?cycle_start_from=202412&cycle_start_to=202412

    # Date Range Query (Day)
    GET /banbury-anomaly-detection?cycle_start_from=2024-12-05&cycle_start_to=2024-12-05

    # Date Range Query (Time Included)
    GET /banbury-anomaly-detection?cycle_start_from=2024-12-05%2014:30:00&cycle_start_to=2024-12-05%2018:30:00
    ```
    """
    try:
        # If no date parameters are provided and no other filter conditions are provided, the default value is today's date.
        has_any_filter = any([
            no, shift is not None, mode, is_anomaly is not None, is_3_stage is not None, result is not None
        ])
        
        if cycle_start_from is None and cycle_start_to is None and not has_any_filter:
            today = date.today()
            today_str = today.strftime("%Y-%m-%d")
            cycle_start_from = today_str
            cycle_start_to = today_str
        
        # Convert date string to datetime
        cycle_start_from_dt, cycle_start_to_dt = _resolve_cycle_start_range(
            cycle_start_from, cycle_start_to
        )

        records = await fetch_banbury_anomaly_results(
            no=no,
            shift=shift,
            mode=mode,
            is_anomaly=is_anomaly,
            is_3_stage=is_3_stage,
            result=result,
            cycle_start_from=cycle_start_from_dt,
            cycle_start_to=cycle_start_to_dt,
        )

        return BanburyAnomalyResultResponse(data=records, total=len(records))

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e

