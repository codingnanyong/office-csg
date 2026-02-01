"""Service layer for MC Master data operations."""

from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session
from app.models.mc_master import McMaster


async def fetch_mc_masters(
    plant_cd: Optional[str] = None,
    process_name: Optional[str] = None,
    op_cd: Optional[str] = None,
    line_no: Optional[int] = None,
    machine_no: Optional[int] = None,
) -> List[McMaster]:
    """Fetch MC Master data from gold.mc_master table
    
    Args:
        plant_cd: filter by plant code (optional, if not provided, all plants)
        process_name: filter by process name (optional, if not provided, all processes)
        op_cd: filter by operation code (optional, if not provided, all operations)
        line_no: filter by line number (optional, if not provided, all lines)
        machine_no: filter by machine number (optional, if not provided, all machines)
    
    Returns:
        MC Master record list
    """
    # only use montrg database
    async with get_session(db_alias="montrg") as session:
        query = select(McMaster)
        
        # add filter conditions
        if plant_cd:
            query = query.where(McMaster.plant_cd == plant_cd)
        if process_name:
            query = query.where(McMaster.process_name == process_name)
        if op_cd:
            query = query.where(McMaster.op_cd == op_cd)
        if line_no is not None:
            query = query.where(McMaster.line_no == line_no)
        if machine_no is not None:
            query = query.where(McMaster.machine_no == machine_no)
        
        result = await session.execute(query)
        return list(result.scalars().all())


def build_hierarchical_structure(mc_masters: List[McMaster]) -> dict:
    """Convert MC Master list to hierarchical structure
    
    Args:
        mc_masters: MC Master record list
    
    Returns:
        Hierarchical structure dictionary (Company → Plant → Process → Operation → Line → Machine)
    """
    from app.schemas.mc_master import (
        Company,
        Plant,
        Process,
        Operation,
        Line,
        Machine,
    )
    
    companies: dict = {}
    
    for mc in mc_masters:
        # Company level
        if mc.company_name not in companies:
            companies[mc.company_name] = Company(
                plants={}
            )
        
        company = companies[mc.company_name]
        
        # Plant level
        if mc.plant_cd not in company.plants:
            company.plants[mc.plant_cd] = Plant(
                processes={}
            )
        
        plant = company.plants[mc.plant_cd]
        
        # Process level
        if mc.process_name not in plant.processes:
            plant.processes[mc.process_name] = Process(
                operations={}
            )
        
        process = plant.processes[mc.process_name]
        
        # Operation level
        if mc.op_cd not in process.operations:
            process.operations[mc.op_cd] = Operation(
                lines={}
            )
        
        operation = process.operations[mc.op_cd]
        
        # Line level
        if mc.line_no not in operation.lines:
            operation.lines[mc.line_no] = Line(
                machines={}
            )
        
        line = operation.lines[mc.line_no]
        
        # Machine level (lowest level)
        line.machines[mc.machine_cd] = Machine(
            machine_no=mc.machine_no
        )
    
    return companies

