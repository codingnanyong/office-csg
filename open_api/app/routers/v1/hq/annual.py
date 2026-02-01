from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from app.database.database import get_db, get_async_db
from app.services.hq.annual_services import (
    annual,
    annual_async,
    last_annual,
    last_annual_async,
    latest_annual,
    latest_annual_async
)
from app.schemas.hq.annual_schema import Annual  

get_hq_db= get_db('hq')
get_hq_async_db = get_async_db('hq')

router = APIRouter()

@router.get("", response_model=list[Annual])
def get_annual(db: Session = Depends(get_hq_db)):
    annuals = annual(db)
    if not annuals:
        raise HTTPException(status_code=404, detail="No annual found")
    return annuals

@router.get("/async", response_model=list[Annual])
async def get_annual_async(db: AsyncSession = Depends(get_hq_async_db)):
    annual = await annual_async(db)
    if not annual:
        raise HTTPException(status_code=404, detail="No annual found")
    return annual

@router.get("/last", response_model=list[Annual])
def get_annual_last(db: Session = Depends(get_hq_db)):
    annual = last_annual(db)
    if not annual:
        raise HTTPException(status_code=404, detail="No annual found")
    return annual

@router.get("/last/async", response_model=list[Annual])
async def get_annual_last_async(db: AsyncSession = Depends(get_hq_async_db)):
    annual = await last_annual_async(db)
    if not annual:
        raise HTTPException(status_code=404, detail="No annual found")
    return annual

@router.get("/latest", response_model=Annual)
def get_annual_latest(db: Session = Depends(get_hq_db)):
    annual = latest_annual(db)
    if not annual:
        raise HTTPException(status_code=404, detail="No annual found")
    return annual

@router.get("/latest/async", response_model=Annual)
async def get_annual_latest_async(db: AsyncSession = Depends(get_hq_async_db)):
    annual = await latest_annual_async(db)
    if not annual:
        raise HTTPException(status_code=404, detail="No annual found")
    return annual