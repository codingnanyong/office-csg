from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from app.database.database import get_db, get_async_db
from app.services.hq.sample_services import ( 
    sample,
    sample_async,
    sample_by_keyword,
    sample_by_keyword_async,
    sample_by_opcd,
    sample_by_opcd_async,
    sample_by_opcd_status,
    sample_by_opcd_status_async
)
from app.schemas.hq.ws_schema import ws

get_hq_db= get_db('hq')
get_hq_async_db = get_async_db('hq')

router = APIRouter()

# Sample

@router.get("/search", response_model=list[ws])
def get_sample_by_keyword(keyword: str = Query(...), db: Session = Depends(get_hq_db)):
    samples = sample_by_keyword(db, keyword)
    if not samples:
        raise HTTPException(status_code=404, detail="No Sample found")
    return samples

@router.get("/search/async", response_model=list[ws])
async def get_sample_by_keyword_async(keyword: str = Query(...),  db: AsyncSession = Depends(get_hq_async_db)):
    samples = await sample_by_keyword_async(db, keyword)
    if not samples:
        raise HTTPException(status_code=404, detail="No Sample found")
    return samples

@router.get("/progress", response_model=list[ws])
def get_sample_by_opcd(opcd: str = Query(...), db: Session = Depends(get_hq_db)):
    samples = sample_by_opcd(db, opcd)
    if not samples:
        raise HTTPException(status_code=404, detail="No Sample found")
    return samples

@router.get("/progress/async", response_model=list[ws])
async def get_sample_by_opcd_async(opcd: str = Query(...),  db: AsyncSession = Depends(get_hq_async_db)):
    samples = await sample_by_opcd_async(db, opcd)
    if not samples:
        raise HTTPException(status_code=404, detail="No Sample found")
    return samples

@router.get("/progress/status", response_model=list[ws])
def get_sample_by_opcd_status(opcd: str = Query(...),status: str = Query(...), db: Session = Depends(get_hq_db)):
    samples = sample_by_opcd_status(db, opcd,status)
    if not samples:
        raise HTTPException(status_code=404, detail="No Sample found")
    return samples

@router.get("/progress/status/async", response_model=list[ws])
async def get_sample_by_opcd_status_async(opcd: str = Query(...),  status: str = Query(...),db: AsyncSession = Depends(get_hq_async_db)):
    samples = await sample_by_opcd_status_async(db, opcd,status)
    if not samples:
        raise HTTPException(status_code=404, detail="No Sample found")
    return samples


@router.get("/{wsno}", response_model=list[ws])
def get_sample(wsno: str, db: Session = Depends(get_hq_db)):
    samples = sample(db, wsno)
    if not samples:
        raise HTTPException(status_code=404, detail="No Sample found")
    return samples


@router.get("/{wsno}/async", response_model=list[ws])
async def get_sample_async(wsno: str, db: AsyncSession = Depends(get_hq_async_db)):
    samples = await sample_async(db, wsno)
    if not samples:
        raise HTTPException(status_code=404, detail="No Sample found")
    return samples
