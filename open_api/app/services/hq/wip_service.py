from sqlalchemy import func, case
from sqlalchemy.future import select 
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.helpers.wip_helper import build_wip,build_wip_async
from app.services.helpers.worklist_helper import build_worklist, build_worklist_async
from app.models.hq.ws_status_model import wsStatusSchema
from app.schemas.hq.ws_list_schema import wsList
from app.schemas.hq.wip_schema import Wip  

# wip

def wip(db: Session):
    results = build_wip(db).all()
    return [Wip(**dict(row._mapping)) for row in results]

async def wip_async(db: AsyncSession):
    stmt = build_wip_async()
    result = await db.execute(stmt)
    rows = result.all()
    return [Wip(**dict(row._mapping)) for row in rows]

def wip_by_opcd(db: Session, opcd: str):
    results = build_wip(db, opcd=opcd).all()
    return [Wip(**dict(row._mapping)) for row in results]

async def wip_by_opcd_async(db: AsyncSession, opcd: str):
    stmt = build_wip_async(opcd=opcd)
    result = await db.execute(stmt)
    rows = result.all()
    return [Wip(**dict(row._mapping)) for row in rows]

# worklist

def wip_worklist(db: Session):
    q = build_worklist(db)
    q = q.filter(
        wsStatusSchema.prod_date == func.to_char(func.now(), 'YYYYMMDD'),
        wsStatusSchema.status == 'I'
    )
    results = q.all()
    return [wsList(**{k: (v if v is not None else "") for k, v in dict(row._mapping).items()}) for row in results]

async def wip_worklist_async(db: AsyncSession):
    stmt = build_worklist_async()
    stmt = stmt.filter(
        wsStatusSchema.prod_date == func.to_char(func.now(), 'YYYYMMDD'),
        wsStatusSchema.status == 'I'
    )
    result = await db.execute(stmt)
    rows = result.all()
    return [wsList(**{k: (v if v is not None else "") for k, v in dict(row._mapping).items()}) for row in rows]

def wip_worklist_by_opcd(db: Session, opcd: str):
    q = build_worklist(db, opcd=opcd)
    q = q.filter(
        wsStatusSchema.prod_date == func.to_char(func.now(), 'YYYYMMDD'),
        wsStatusSchema.status == 'I'
    )
    results = q.all()
    return [wsList(**{k: (v if v is not None else "") for k, v in dict(row._mapping).items()}) for row in results]

async def wip_worklist_by_opcd_async(db: AsyncSession, opcd: str):
    stmt = build_worklist_async(opcd=opcd)
    stmt = stmt.filter(
        wsStatusSchema.prod_date == func.to_char(func.now(), 'YYYYMMDD'),
        wsStatusSchema.status == 'I'
    )
    result = await db.execute(stmt)
    rows = result.all()
    return [wsList(**{k: (v if v is not None else "") for k, v in dict(row._mapping).items()}) for row in rows]

def wip_worklist_by_keyword(db: Session, keyword: str):
    q = build_worklist(db, keyword=keyword)
    q = q.filter(
        wsStatusSchema.prod_date == func.to_char(func.now(), 'YYYYMMDD'),
        wsStatusSchema.status == 'I'
    )
    results = q.all()
    return [wsList(**{k: (v if v is not None else "") for k, v in dict(row._mapping).items()}) for row in results]

async def wip_worklist_by_keyword_async(db: AsyncSession, keyword: str):
    stmt = build_worklist_async(keyword=keyword)
    stmt = stmt.filter(
        wsStatusSchema.prod_date == func.to_char(func.now(), 'YYYYMMDD'),
        wsStatusSchema.status == 'I'
    )
    result = await db.execute(stmt)
    rows = result.all()
    return [wsList(**{k: (v if v is not None else "") for k, v in dict(row._mapping).items()}) for row in rows]