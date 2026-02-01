from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func
from app.models.hq.annual_model import AnnualSchema

def annual(db: Session):
    return db.query(AnnualSchema).all()

async def annual_async(db: AsyncSession):
    result = await db.execute(select(AnnualSchema))
    return result.scalars().all()

def last_annual(db: Session):
    current_year = db.scalar(select(func.extract('year', func.now())))
    return db.query(AnnualSchema).filter(AnnualSchema.year >= current_year - 2).all()

async def last_annual_async(db: AsyncSession):
    result = await db.execute(select(func.extract('year', func.now())))
    current_year = result.scalar_one_or_none()
    if current_year is None:
        return []
    result = await db.execute(select(AnnualSchema).filter(AnnualSchema.year >= current_year - 2))
    return result.scalars().all()

def latest_annual(db: Session):
    current_year = db.scalar(select(func.extract('year', func.now())))
    if current_year is None:
        return None
    return db.query(AnnualSchema).filter(AnnualSchema.year == current_year).first()

async def latest_annual_async(db: AsyncSession):
    result = await db.execute(select(func.extract('year', func.now())))
    current_year = result.scalar_one_or_none()
    if current_year is None:
        return None
    result = await db.execute(select(AnnualSchema).filter(AnnualSchema.year == current_year).limit(1))
    return result.scalar_one_or_none()
