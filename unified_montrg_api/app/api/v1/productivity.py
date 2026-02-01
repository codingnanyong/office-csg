"""Endpoints for querying Productivity data."""
from fastapi import APIRouter, HTTPException, Query
from datetime import date, datetime
from typing import Optional

from app.schemas.productivity_op_cd import ProductivityOpCdResponse
from app.schemas.productivity_op_group import ProductivityOpGroupResponse
from app.schemas.productivity_ip_zone import ProductivityIpZoneResponse
from app.schemas.productivity_ip_machine import ProductivityIpMachineResponse
from app.schemas.productivity_ph_zone import ProductivityPhZoneResponse
from app.schemas.productivity_ph_machine import ProductivityPhMachineResponse
from app.services.productivity_op_cd import fetch_productivity_op_cd
from app.services.productivity_op_group import fetch_productivity_op_group
from app.services.productivity_ip_zone import fetch_productivity_ip_zone
from app.services.productivity_ip_machine import fetch_productivity_ip_machine
from app.services.productivity_ph_zone import fetch_productivity_ph_zone
from app.services.productivity_ph_machine import fetch_productivity_ph_machine

router = APIRouter(prefix="/productivity", tags=["productivity"])


def parse_date(date_str: str) -> date:
    """Parse date string to date object (yyyyMMdd or yyyy-MM-dd format supported)"""
    try:
        # Try yyyy-MM-dd format
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        try:
            # Try yyyyMMdd format
            return datetime.strptime(date_str, "%Y%m%d").date()
        except ValueError:
            raise ValueError(f"Invalid date format: {date_str}. Use yyyy-MM-dd or yyyyMMdd format")


@router.get("/op-cd", response_model=ProductivityOpCdResponse, summary="Productivity OP CD Data Query")
async def get_productivity_op_cd(
    op_cd: Optional[str] = Query(None, description="Operation Code Filtering (Optional, if not provided, all op_cd)"),
    date: Optional[str] = Query(
        None,
        description="Business Date Filtering (yyyyMMdd or yyyy-MM-dd format, default: realtime today when omitted)",
    ),
) -> ProductivityOpCdResponse:
    """Retrieve Productivity OP CD data with filters.
    
    Filter Conditions (All Optional):
    - op_cd: Operation Code Filtering (Optional, if not provided, all op_cd)
    - date: Business Date Filtering (Optional, omit to return today's real-time snapshot)
    
    Examples:
    - All (real-time): GET /productivity/op-cd
    - Specific op_cd (real-time): GET /productivity/op-cd?op_cd=OP001
    - date included (yyyy-MM-dd): GET /productivity/op-cd?op_cd=OP001&date=2025-01-01
    - date included (yyyyMMdd): GET /productivity/op-cd?op_cd=OP001&date=20250101
    
    Returns:
        List of Productivity OP CD data and total record count
    """
    try:
        parsed_date = parse_date(date) if date else None
        
        records = await fetch_productivity_op_cd(
            op_cd=op_cd,
            date=parsed_date,
        )
        
        return ProductivityOpCdResponse(
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


@router.get("/op-group", response_model=ProductivityOpGroupResponse, summary="Productivity OP Group Data Query")
async def get_productivity_op_group(
    op_group: Optional[str] = Query(None, description="Operation Group Filtering (Optional, if not provided, all op_group)"),
    date: Optional[str] = Query(
        None,
        description="Business Date Filtering (yyyyMMdd or yyyy-MM-dd format, default: realtime today when omitted)",
    ),
) -> ProductivityOpGroupResponse:
    """Retrieve Productivity OP Group data with filters.
    
    필터 조건:
    - op_group: Operation Group Filtering (Optional, if not provided, all op_group)
    - date: Business Date Filtering (Optional, omit to return today's real-time snapshot)
    
    예시:
    - All (real-time): GET /productivity/op-group
    - Specific op_group (real-time): GET /productivity/op-group?op_group=GROUP_A
    - date included (yyyy-MM-dd): GET /productivity/op-group?op_group=GROUP_A&date=2025-01-01
    - date included (yyyyMMdd): GET /productivity/op-group?op_group=GROUP_A&date=20250101
    
    Returns:
        List of Productivity OP Group data and total record count
    """
    try:
        parsed_date = parse_date(date) if date else None
        
        records = await fetch_productivity_op_group(
            op_group=op_group,
            date=parsed_date,
        )
        
        return ProductivityOpGroupResponse(
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


@router.get("/ip-zone", response_model=ProductivityIpZoneResponse, summary="Productivity IP Zone Data Query")
async def get_productivity_ip_zone(
    zone_cd: Optional[str] = Query(None, description="Zone Code Filtering (Optional, if not provided, all zone)"),
    date: Optional[str] = Query(
        None,
        description="Business Date Filtering (yyyyMMdd or yyyy-MM-dd format, default: realtime today when omitted)",
    ),
) -> ProductivityIpZoneResponse:
    """Retrieve Productivity IP Zone data with filters.
    
    Filter Conditions (All Optional):
    - zone_cd: Zone Code Filtering (Optional, if not provided, all zone)
    - date: Business Date Filtering (Optional, omit to return today's real-time snapshot)
    
    Examples:
    - All (real-time): GET /productivity/ip-zone
    - Specific zone (real-time): GET /productivity/ip-zone?zone_cd=ZONE001
    - date included (yyyy-MM-dd): GET /productivity/ip-zone?zone_cd=ZONE001&date=2025-01-01
    - date included (yyyyMMdd): GET /productivity/ip-zone?zone_cd=ZONE001&date=20250101
    
    Returns:
        List of Productivity IP Zone data and total record count
    """
    try:
        parsed_date = parse_date(date) if date else None
        
        records = await fetch_productivity_ip_zone(
            zone_cd=zone_cd,
            date=parsed_date,
        )
        
        return ProductivityIpZoneResponse(
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


@router.get("/ph-line", response_model=ProductivityPhZoneResponse, summary="Productivity PH Line Data Query")
async def get_productivity_ph_line(
    line_group: Optional[str] = Query(None, description="Line Group Filtering (Optional, if not provided, all line_group)"),
    date: Optional[str] = Query(
        None,
        description="Business Date Filtering (yyyyMMdd or yyyy-MM-dd format, default: realtime today when omitted)",
    ),
) -> ProductivityPhZoneResponse:
    """Retrieve Productivity PH Line data with filters.
    
    Filter Conditions (All Optional):
    - line_group: Line Group Filtering (Optional, if not provided, all line_group)
    - date: Business Date Filtering (Optional, omit to return today's real-time snapshot)
    
    Examples:
    - All (real-time): GET /productivity/ph-line
    - Specific line_group (real-time): GET /productivity/ph-line?line_group=LINE001
    - date included (yyyy-MM-dd): GET /productivity/ph-line?line_group=LINE001&date=2025-01-01
    - date included (yyyyMMdd): GET /productivity/ph-line?line_group=LINE001&date=20250101
    
    Returns:
        List of Productivity PH Line data and total record count
    """
    try:
        parsed_date = parse_date(date) if date else None
        
        records = await fetch_productivity_ph_zone(
            line_group=line_group,
            date=parsed_date,
        )
        
        return ProductivityPhZoneResponse(
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


@router.get("/ip-machine", response_model=ProductivityIpMachineResponse, summary="Productivity IP Machine Data Query")
async def get_productivity_ip_machine(
    machine_cd: Optional[str] = Query(None, description="Machine Code Filtering (Optional, if not provided, all machine)"),
    date: Optional[str] = Query(
        None,
        description="Business Date Filtering (yyyyMMdd or yyyy-MM-dd format, default: realtime today when omitted)",
    ),
) -> ProductivityIpMachineResponse:
    """Retrieve Productivity IP Machine data with filters.
    
    Filter Conditions (All Optional):
    - machine_cd: Machine Code Filtering (Optional, if not provided, all machine)
    - date: Business Date Filtering (Optional, omit to return today's real-time snapshot)
    
    Examples:
    - All (real-time): GET /productivity/ip-machine
    - Specific machine (real-time): GET /productivity/ip-machine?machine_cd=MCA02
    - date included (yyyy-MM-dd): GET /productivity/ip-machine?machine_cd=MCA02&date=2025-01-01
    - date included (yyyyMMdd): GET /productivity/ip-machine?machine_cd=MCA02&date=20250101
    
    Returns:
        List of Productivity IP Machine data and total record count
    """
    try:
        parsed_date = parse_date(date) if date else None
        
        records = await fetch_productivity_ip_machine(
            machine_cd=machine_cd,
            date=parsed_date,
        )
        
        return ProductivityIpMachineResponse(
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


@router.get("/ph-machine", response_model=ProductivityPhMachineResponse, summary="Productivity PH Machine Data Query")
async def get_productivity_ph_machine(
    machine_cd: Optional[str] = Query(None, description="Machine Code Filtering (Optional, if not provided, all machine)"),
    date: Optional[str] = Query(
        None,
        description="Business Date Filtering (yyyyMMdd or yyyy-MM-dd format, default: realtime today when omitted)",
    ),
) -> ProductivityPhMachineResponse:
    """Retrieve Productivity PH Machine data with filters.
    
    Filter Conditions (All Optional):
    - machine_cd: Machine Code Filtering (Optional, if not provided, all machine)
    - date: Business Date Filtering (Optional, omit to return today's real-time snapshot)
    
    Examples:
    - All (real-time): GET /productivity/ph-machine
    - Specific machine (real-time): GET /productivity/ph-machine?machine_cd=PH01
    - date included (yyyy-MM-dd): GET /productivity/ph-machine?machine_cd=PH01&date=2025-01-01
    - date included (yyyyMMdd): GET /productivity/ph-machine?machine_cd=PH01&date=20250101
    
    Returns:
        List of Productivity PH Machine data and total record count
    """
    try:
        parsed_date = parse_date(date) if date else None
        
        records = await fetch_productivity_ph_machine(
            machine_cd=machine_cd,
            date=parsed_date,
        )
        
        return ProductivityPhMachineResponse(
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
