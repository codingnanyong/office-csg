from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.hq.tooling_warehouse_schema import ToolingWarehouse
from app.services.helpers.tooling_helper import get_tooling_data,get_tooling_data_async

def warehouse_tooling(db : Session) -> list[ToolingWarehouse]:
    result = get_tooling_data(db)
    return result

async def warehouse_tooling_async(db : AsyncSession) -> list[ToolingWarehouse]:
    result = await get_tooling_data_async(db)
    return result

def warehouse_tooling_by_loc(db : Session, loc : str = None) -> ToolingWarehouse:
    result = get_tooling_data(db,loc)
    return result

async def warehouse_tooling_by_loc_async(db : Session, loc : str = None) -> ToolingWarehouse:
    result = await get_tooling_data_async(db,loc)
    return result