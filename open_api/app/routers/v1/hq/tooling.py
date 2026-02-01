from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from app.database.database import get_db, get_async_db
from app.services.hq.tooling_services import (
    warehouse_tooling,
    warehouse_tooling_async,
    warehouse_tooling_by_loc,
    warehouse_tooling_by_loc_async
)
from app.schemas.hq.tooling_warehouse_schema import ToolingWarehouse

get_hq_db= get_db('hq')
get_hq_async_db = get_async_db('hq')

router = APIRouter()

@router.get('/',response_model = list[ToolingWarehouse])
def get_tooling_warehouse(db : Session = Depends(get_hq_db)):
    toolings = warehouse_tooling(db)
    if not toolings:
        raise HTTPException(status_code=404, detail="No Tooling found")
    return toolings

@router.get('/async',response_model = list[ToolingWarehouse])
async def get_tooling_warehouse(db : AsyncSession = Depends(get_hq_async_db)):
    toolings = await warehouse_tooling_async(db)
    if not toolings:
        raise HTTPException(status_code=404, detail="No Tooling found")
    return toolings

@router.get('/locaction',response_model = ToolingWarehouse)
def get_tooling_warehouse_by_loc(loc: str= None, db : Session = Depends(get_hq_db)):
    toolings = warehouse_tooling_by_loc(db,loc)
    if not toolings:
        raise HTTPException(status_code=404, detail="No Tooling found")
    return toolings

@router.get('/locaction/async',response_model = ToolingWarehouse)
async def get_tooling_warehouse_by_loc_async(loc: str= None, db : AsyncSession = Depends(get_hq_async_db)):
    toolings = await warehouse_tooling_by_loc_async(db,loc)
    if not toolings:
        raise HTTPException(status_code=404, detail="No Tooling found")
    return toolings