"""Endpoints for querying MC Master data."""
from fastapi import APIRouter, HTTPException, Query

from app.schemas.mc_master import Machines
from app.services.mc_master import build_hierarchical_structure, fetch_mc_masters

router = APIRouter(prefix="/machines", tags=["machines"])


@router.get("", response_model=Machines, summary="Get MC Master data with filters")
async def get_machines(
    plant_cd: str | None = Query(None, description="Plant Code Filtering (Optional, if not provided, all plants)"),
    process_name: str | None = Query(None, description="Process Name Filtering (Optional, if not provided, all processes)"),
    op_cd: str | None = Query(None, description="Operation Code Filtering (Optional, if not provided, all operations)"),
    line_no: int | None = Query(None, description="Line Number Filtering (Optional, if not provided, all lines)"),
    machine_no: int | None = Query(None, description="Machine Number Filtering (Optional, if not provided, all machines)"),
) -> Machines:
    """Retrieve MC Master data in hierarchical structure.
    
    Filter Conditions (All Optional):
    - plant_cd: Plant Code Filtering (Optional, if not provided, all plants)
    - process_name: Process Name Filtering (Optional, if not provided, all processes)
    - op_cd: Operation Code Filtering (Optional, if not provided, all operations)
    - line_no: Line Number Filtering (Optional, if not provided, all lines)
    - machine_no: Machine Number Filtering (Optional, if not provided, all machines)
    
    Combination Examples:
    - All: GET /machines
    - Plant-wise: GET /machines?plant_cd=PLANT01
    - Process-wise: GET /machines?process_name=PROCESS01
    - Operation-wise: GET /machines?op_cd=OP01
    - Operation+Line: GET /machines?op_cd=OP01&line_no=1
    - Operation+Machine: GET /machines?op_cd=OP01&machine_no=1
    - Plant+Process: GET /machines?plant_cd=PLANT01&process_name=PROCESS01
    
    Return Format:
    - Company → Plant → Process → Operation → Line → Machine (Hierarchical Structure)
    """
    try:
        mc_masters = await fetch_mc_masters(
            plant_cd=plant_cd,
            process_name=process_name,
            op_cd=op_cd,
            line_no=line_no,
            machine_no=machine_no,
        )
        
        if not mc_masters:
            raise HTTPException(status_code=404, detail="No data found for the provided filters")
        
        companies_dict = build_hierarchical_structure(mc_masters)
        
        return Machines(companies=companies_dict)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e
