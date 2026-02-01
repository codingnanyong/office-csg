from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from app.database.database import get_db, get_async_db
from app.services.hq.dailystatus_services import (
    dailystatus,
    dailystatus_async,
    dailystatus_worklist_by_opcd,
    dailystatus_worklist_by_opcd_async,
    dailystatus_worklist_by_keyword,
    dailystatus_worklist_by_keyword_async
)
from app.schemas.hq.dailystatus_schema import Dailystatus  
from app.schemas.hq.ws_list_schema import wsList

get_hq_db= get_db('hq')
get_hq_async_db = get_async_db('hq')

router = APIRouter()

# dailystatus

@router.get("", response_model=list[Dailystatus])
def get_dailystatus(db: Session = Depends(get_hq_db)):
    status = dailystatus(db)
    if not status:
        raise HTTPException(status_code=404, detail="No dailystatus found")
    return status

@router.get("/async", response_model=list[Dailystatus])
async def get_dailystatus_async(db: AsyncSession = Depends(get_hq_async_db)):
    dailystatus = await dailystatus_async(db)
    if not dailystatus:
        raise HTTPException(status_code=404, detail="No dailystatus found")
    return dailystatus

# worklist

@router.get("/worklist/search", response_model=list[wsList])
def get_worklist_by_keyword(keyword: str = Query(...), db: Session = Depends(get_hq_db)):
    worklist = dailystatus_worklist_by_keyword(db, keyword)
    if not worklist:
        raise HTTPException(status_code=404, detail=f"No worklist found for keyword: {keyword}")
    return worklist

@router.get("/worklist/search/async", response_model=list[wsList])
async def get_worklist_by_keyword_async(keyword: str = Query(...), db: AsyncSession = Depends(get_hq_async_db)):
    worklist = await dailystatus_worklist_by_keyword_async(db, keyword)
    if not worklist:
        raise HTTPException(status_code=404, detail=f"No worklist found for keyword: {keyword}")
    return worklist

@router.get("/worklist/{opcd}", response_model=list[wsList])
def get_worklist_by_opcd(opcd: str, db: Session = Depends(get_hq_db)):
    worklist = dailystatus_worklist_by_opcd(db, opcd)
    if not worklist:
        raise HTTPException(status_code=404, detail=f"No worklist found for opcd: {opcd}")
    return worklist

@router.get("/worklist/{opcd}/async", response_model=list[wsList])
async def get_worklist_by_opcd_async(opcd: str, db: AsyncSession = Depends(get_hq_async_db)):
    worklist = await dailystatus_worklist_by_opcd_async(db, opcd)
    if not worklist:
        raise HTTPException(status_code=404, detail=f"No worklist found for opcd: {opcd}")
    return worklist