"""Service layer for Work Order analysis operations."""

from datetime import datetime
from typing import Dict, Optional, Any
from sqlalchemy import text
from app.core.database import get_session
from app.schemas.wo_analysis import (
    ProcessWoStatus,
    OperationWoStatus,
    LineWoStatus,
    MachineWoStatus,
    WoStatusCount,
)

async def _execute_query(query: text, start_date: str, end_date: str) -> list:
    """Common query execution function"""
    async with get_session(db_alias="montrg") as session:
        result = await session.execute(
            query,
            {
                "start_date": datetime.strptime(start_date, "%Y-%m-%d").date(),
                "end_date": datetime.strptime(end_date, "%Y-%m-%d").date(),
            }
        )
        return result.all()


def _get_query_by_group(group_by: str) -> text:
    """Dynamically create query based on group_by"""
    # define GROUP BY clause
    group_by_clauses = {
        "machine": ["mm.process_name", "mm.op_cd", "mm.line_no", "mm.machine_cd", "wor.wo_status"],
        "op_line": ["mm.process_name", "mm.op_cd", "mm.line_no", "wor.wo_status"],
        "op": ["mm.process_name", "mm.op_cd", "wor.wo_status"],
        "process": ["mm.process_name", "wor.wo_status"],
    }
    
    if group_by not in group_by_clauses:
        raise ValueError(f"Invalid group_by value: {group_by}")
    
    group_by_list = group_by_clauses[group_by]
    group_by_str = ", ".join(group_by_list)
    order_by_str = ", ".join(group_by_list)
    
    # SELECT clause is the same as GROUP BY clause (except COUNT)
    select_list = [col for col in group_by_list] + ["COUNT(*) as count"]
    select_str = ",\n                ".join(select_list)
    
    query_str = f"""
            SELECT 
                {select_str}
            FROM bronze.mc_master mm
            LEFT JOIN bronze.wof_order_raw wor ON mm.machine_cd = wor.mach_id
            WHERE wor.upd_date BETWEEN :start_date AND :end_date
            GROUP BY {group_by_str}
            ORDER BY {order_by_str}
        """
    
    return text(query_str)


def _build_hierarchical_structure(rows: list, group_by: str) -> Dict[str, ProcessWoStatus]:
    """Convert row data to hierarchical structure"""
    processes: Dict[str, ProcessWoStatus] = {}
    
    for row in rows:
        process_name = row.process_name or "UNKNOWN"
        wo_status = row.wo_status or "UNKNOWN"
        count = row.count
        
        # Process level
        if process_name not in processes:
            processes[process_name] = ProcessWoStatus(
                operations={} if group_by != "process" else {},
                wo_statuses={} if group_by == "process" else {}
            )
        
        process = processes[process_name]
        
        if group_by == "process":
            process.wo_statuses[wo_status] = WoStatusCount(wo_status=wo_status, count=count)
            continue
        
        op_cd = row.op_cd or "UNKNOWN"
        
        # Operation level
        if op_cd not in process.operations:
            process.operations[op_cd] = OperationWoStatus(
                lines={} if group_by in ("machine", "op_line") else {},
                wo_statuses={} if group_by == "op" else {}
            )
        
        operation = process.operations[op_cd]
        
        if group_by == "op":
            operation.wo_statuses[wo_status] = WoStatusCount(wo_status=wo_status, count=count)
            continue
        
        line_no = row.line_no
        
        # Line level
        if line_no not in operation.lines:
            operation.lines[line_no] = LineWoStatus(
                machines={} if group_by == "machine" else {},
                wo_statuses={} if group_by == "op_line" else {}
            )
        
        line = operation.lines[line_no]
        
        if group_by == "op_line":
            line.wo_statuses[wo_status] = WoStatusCount(wo_status=wo_status, count=count)
            continue
        
        # Machine level (group_by == "machine")
        machine_cd = row.machine_cd or "UNKNOWN"
        
        if machine_cd not in line.machines:
            line.machines[machine_cd] = MachineWoStatus(wo_statuses={})
        
        machine = line.machines[machine_cd]
        machine.wo_statuses[wo_status] = WoStatusCount(wo_status=wo_status, count=count)
    
    return processes


async def analyze_wo(
    start_date: str,
    end_date: str,
    group_by: str,
) -> Dict[str, ProcessWoStatus]:
    """Fetch Work Order Analysis data with hierarchical structure (common function)
    
    Args:
        start_date: start date (YYYY-MM-DD)
        end_date: end date (YYYY-MM-DD)
        group_by: grouping method (machine, op_line, op, process)
    
    Returns:
        dictionary with process_name as key and hierarchical structure as value
    """
    query = _get_query_by_group(group_by)
    if query is None:
        raise ValueError(f"Invalid group_by value: {group_by}")
    
    rows = await _execute_query(query, start_date, end_date)
    return _build_hierarchical_structure(rows, group_by)


# keep individual functions calling the common function for backward compatibility
async def analyze_wo_by_machine(
    start_date: str,
    end_date: str,
) -> Dict[str, ProcessWoStatus]:
    """Fetch Work Order Analysis data by machine (process_name, op_cd, line_no, machine_cd, wo_status)"""
    return await analyze_wo(start_date, end_date, "machine")


async def analyze_wo_by_op_line(
    start_date: str,
    end_date: str,
) -> Dict[str, ProcessWoStatus]:
    """Fetch Work Order Analysis data by op_line (process_name, op_cd, line_no, wo_status)"""
    return await analyze_wo(start_date, end_date, "op_line")


async def analyze_wo_by_op(
    start_date: str,
    end_date: str,
) -> Dict[str, ProcessWoStatus]:
    """Fetch Work Order Analysis data by op (process_name, op_cd, wo_status)"""
    return await analyze_wo(start_date, end_date, "op")


async def analyze_wo_by_process(
    start_date: str,
    end_date: str,
) -> Dict[str, ProcessWoStatus]:
    """Fetch Work Order Analysis data by process (process_name, wo_status)"""
    return await analyze_wo(start_date, end_date, "process")
