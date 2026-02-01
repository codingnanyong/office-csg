from sqlalchemy.orm import Session
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.hq.dailystatus_model import DailystatusSchema
from app.schemas.hq.ws_list_schema import wsList
from app.services.helpers.worklist_helper import build_worklist, build_worklist_async

# dailystatus

def dailystatus(db: Session):
    return db.query(DailystatusSchema).all()

async def dailystatus_async(db: AsyncSession):
    result = await db.execute(select(DailystatusSchema))
    return result.scalars().all()

# worklist 
def dailystatus_worklist(db: Session):
    results = build_worklist(db).all()
    return [wsList(**dict(row._mapping)) for row in results]

def dailystatus_worklist_by_opcd(db: Session, opcd: str):
    results = build_worklist(db, opcd=opcd).all()
    return [wsList(**dict(row._mapping)) for row in results]

def dailystatus_worklist_by_keyword(db: Session, keyword: str):
    results = build_worklist(db, keyword=keyword).all()
    return [wsList(**dict(row._mapping)) for row in results]

async def dailystatus_worklist_async(db: AsyncSession):
    stmt = build_worklist_async()
    result = await db.execute(stmt)
    rows = result.all()
    return [wsList(**dict(row._mapping)) for row in rows]

async def dailystatus_worklist_by_opcd_async(db: AsyncSession, opcd: str):
    stmt = build_worklist_async(opcd=opcd)
    result = await db.execute(stmt)
    rows = result.all()
    return [wsList(**dict(row._mapping)) for row in rows]

async def dailystatus_worklist_by_keyword_async(db: AsyncSession, keyword: str):
    stmt = build_worklist_async(keyword=keyword)
    result = await db.execute(stmt)
    rows = result.all()
    return [wsList(**dict(row._mapping)) for row in rows]