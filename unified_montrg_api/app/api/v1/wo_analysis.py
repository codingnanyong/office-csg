"""Endpoints for Work Order analysis."""
from fastapi import APIRouter, HTTPException, Query
from datetime import datetime

from app.schemas.wo_analysis import WorkOrderAnalysisResponse
from app.services.wo_analysis import (
    analyze_wo_by_machine,
    analyze_wo_by_op_line,
    analyze_wo_by_op,
    analyze_wo_by_process,
)

router = APIRouter(prefix="/wo-analysis", tags=["wo-analysis"])


@router.get(
    "",
    response_model=WorkOrderAnalysisResponse,
    summary="Retrieve Work Order Analysis data with dynamic grouping.",
)
async def get_wo_analysis(
    start_date: str = Query(..., description="Start Date (YYYY-MM-DD)", pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str = Query(..., description="End Date (YYYY-MM-DD)", pattern=r"^\d{4}-\d{2}-\d{2}$"),
    group_by: str = Query(
        "op",
        description="Grouping Method: machine (process_name -> op_cd -> line_no -> machine_cd -> wo_status), op_line (process_name -> op_cd -> line_no -> wo_status), op (process_name -> op_cd -> wo_status), process (process_name -> wo_status)",
        pattern=r"^(machine|op_line|op|process)$",
    ),
) -> WorkOrderAnalysisResponse:
    """Retrieve Work Order Analysis data with hierarchical structure.
    
    Grouping Method:
    - `machine`: process_name -> op_cd -> line_no -> machine_cd -> wo_status by count
    - `op_line`: process_name -> op_cd -> line_no -> wo_status by count
    - `op`: process_name -> op_cd -> wo_status by count
    - `process`: process_name -> wo_status by count
    
    Examples:
    - GET /wo-analysis?start_date=2025-11-01&end_date=2025-11-30&group_by=machine
    - GET /wo-analysis?start_date=2025-11-01&end_date=2025-11-30&group_by=op_line
    - GET /wo-analysis?start_date=2025-11-01&end_date=2025-11-30&group_by=op
    - GET /wo-analysis?start_date=2025-11-01&end_date=2025-11-30&group_by=process
    
    Returns:
        Hierarchical structure converted Work Order Analysis result
    """
    try:
        # Validate date format
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start_dt > end_dt:
            raise HTTPException(status_code=400, detail="start_date must be earlier than or equal to end_date")
        
        # Call different functions based on grouping method
        if group_by == "machine":
            processes = await analyze_wo_by_machine(start_date=start_date, end_date=end_date)
        elif group_by == "op_line":
            processes = await analyze_wo_by_op_line(start_date=start_date, end_date=end_date)
        elif group_by == "op":
            processes = await analyze_wo_by_op(start_date=start_date, end_date=end_date)
        elif group_by == "process":
            processes = await analyze_wo_by_process(start_date=start_date, end_date=end_date)
        else:
            raise HTTPException(status_code=400, detail=f"Invalid group_by value: {group_by}. Must be one of: machine, op_line, op, process")
        
        return WorkOrderAnalysisResponse(
            processes=processes,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}") from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e

