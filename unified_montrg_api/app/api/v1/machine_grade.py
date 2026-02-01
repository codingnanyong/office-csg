"""Endpoints for querying Machine Grade data."""
from datetime import date
from fastapi import APIRouter, HTTPException, Query

from app.schemas.machine_grade import MachineGradeItem, MachineGradeResponse
from app.services.machine_grade import fetch_machine_grade

router = APIRouter(prefix="/machine-grade", tags=["machine-grade"])


@router.get("", response_model=MachineGradeResponse, summary="Get Machine Grade data with filters")
async def get_machine_grade(
    mc_nm: str | None = Query(None, description="Machine Name Filtering (Optional, if not provided, all machines)"),
    ym: date | None = Query(None, description="Year Month Filtering (YYYY-MM-DD)"),
    grade: str | None = Query(None, description="Grade Filtering (Optional, if not provided, all grades)"),
) -> MachineGradeResponse:
    """Retrieve machine grade data with filters.
    
    Filter Conditions (All Optional):
    - mc_nm: Machine Name Filtering (Optional, if not provided, all machines)
    - ym: Year Month Filtering (YYYY-MM-DD)
    - grade: Grade Filtering (Optional, if not provided, all grades)
    
    Returns:
        List of machine grade data and total record count
    """
    try:
        items = await fetch_machine_grade(
            mc_nm=mc_nm,
            ym=ym,
            grade=grade,
        )
        
        if not items:
            raise HTTPException(status_code=404, detail="No data found for the provided filters")
        
        return MachineGradeResponse(
            items=[MachineGradeItem.model_validate(item) for item in items],
            count=len(items)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e

