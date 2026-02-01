from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from app.database.database import get_db, get_async_db
from app.services.hq.wip_service import ( 
    wip,
    wip_async,
    wip_by_opcd,
    wip_by_opcd_async,
    wip_worklist_by_opcd,
    wip_worklist_by_opcd_async,
    wip_worklist_by_keyword,
    wip_worklist_by_keyword_async
)
from app.schemas.hq.wip_schema import Wip 
from app.schemas.hq.ws_list_schema import wsList

get_hq_db= get_db('hq')
get_hq_async_db = get_async_db('hq')

router = APIRouter()

# wip

@router.get("", response_model=list[Wip])
def get_wip(db: Session = Depends(get_hq_db)):
    wiplist = wip(db)
    if not wiplist:
        raise HTTPException(status_code=404, detail="No wip found")
    return wiplist

@router.get("/async", response_model=list[Wip])
async def get_wip_async(db: AsyncSession = Depends(get_hq_async_db)):
    wip = await wip_async(db)
    if not wip:
        raise HTTPException(status_code=404, detail="No wip found")
    return wip

@router.get("/{opcd}", response_model=list[Wip])
def get_wip_by_opcd(opcd: str,db: Session = Depends(get_hq_db)):
    wip = wip_by_opcd(db, opcd)
    if not wip:
        raise HTTPException(status_code=404, detail="No wip found")
    return wip

@router.get("/{opcd}/async", response_model=list[Wip])
async def get_wip_opcd_async(opcd: str,db: AsyncSession = Depends(get_hq_async_db)):
    wip = await wip_by_opcd_async(db, opcd)
    if not wip:
        raise HTTPException(status_code=404, detail="No wip found")
    return wip

# worklist

@router.get("/worklist/search", response_model=list[wsList])
def get_worklist_by_keyword(keyword: str = Query(...), db: Session = Depends(get_hq_db)):
    worklist = wip_worklist_by_keyword(db, keyword)
    if not worklist:
        raise HTTPException(status_code=404, detail=f"No worklist found for keyword: {keyword}")
    return worklist

@router.get("/worklist/search/async", response_model=list[wsList])
async def get_worklist_by_keyword_async(keyword: str = Query(...), db: AsyncSession = Depends(get_hq_async_db)):
    worklist = await wip_worklist_by_keyword_async(db, keyword)
    if not worklist:
        raise HTTPException(status_code=404, detail=f"No worklist found for keyword: {keyword}")
    return worklist

@router.get("/worklist/{opcd}", response_model=list[wsList])
def get_worklist_by_opcd(opcd: str, db: Session = Depends(get_hq_db)):
    worklist = wip_worklist_by_opcd(db, opcd)
    if not worklist:
        raise HTTPException(status_code=404, detail=f"No worklist found for opcd: {opcd}")
    return worklist

@router.get("/worklist/{opcd}/async", response_model=list[wsList])
async def get_worklist_by_opcd_async(opcd: str, db: AsyncSession = Depends(get_hq_async_db)):
    worklist = await wip_worklist_by_opcd_async(db, opcd)
    if not worklist:
        raise HTTPException(status_code=404, detail=f"No worklist found for opcd: {opcd}")
    return worklist