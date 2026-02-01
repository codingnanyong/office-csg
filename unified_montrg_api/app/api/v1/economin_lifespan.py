"""Endpoints for querying Economin Lifespan data."""
from fastapi import APIRouter, HTTPException, Query

from app.schemas.economin_lifespan import EconominLifespanItem, EconominLifespanResponse
from app.services.economin_lifespan import fetch_economin_lifespan

router = APIRouter(prefix="/economin-lifespan", tags=["economin-lifespan"])


@router.get("", response_model=EconominLifespanResponse, summary="Get Economin Lifespan data with filters")
async def get_economin_lifespan(
    machine_group: str | None = Query(None, description="Machine Group Filtering (Optional, if not provided, all machine groups)"),
    use_year: int | None = Query(None, description="Use Year Filtering (Optional, if not provided, all use years)"),
) -> EconominLifespanResponse:
    """Retrieve economin lifespan data from the mart_economin_lifespan table.
    
    Filter Conditions (All Optional):
    - machine_group: Filter by machine group (Optional, if not provided, all machine groups)
    - use_year: Filter by use year (Optional, if not provided, all use years)
    """
    try:
        items = await fetch_economin_lifespan(
            machine_group=machine_group,
            use_year=use_year,
        )
        
        if not items:
            raise HTTPException(status_code=404, detail="No data found for the provided filters")
        
        return EconominLifespanResponse(
            items=[EconominLifespanItem.model_validate(item) for item in items],
            count=len(items)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e

