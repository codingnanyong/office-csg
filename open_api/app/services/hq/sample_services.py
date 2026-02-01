from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.helpers.sample_helper import get_sample, get_sample_async
from app.schemas.hq.ws_schema import ws

def sample(db: Session, wsno: str) -> list[ws]:
    result = get_sample(db, wsno=wsno)
    if not result:
        return []
    return result

async def sample_async(db: AsyncSession, wsno: str) -> list[ws]:
    result = await get_sample_async(db, wsno=wsno)
    if not result:
        return []
    return result

def sample_by_keyword(db: Session, keyword: str) -> list[ws]:
    result = get_sample(db, keyword=keyword)
    if not result:
        return []
    return result

async def sample_by_keyword_async(db: AsyncSession, keyword: str) -> list[ws]:
    result = await get_sample_async(db, keyword=keyword)
    if not result:
        return []
    return result

def sample_by_opcd(db: Session, opcd: str) -> list[ws]:
    result = get_sample(db, opcd=opcd)
    if not result:
        return []
    return result

async def sample_by_opcd_async(db: AsyncSession, opcd: str) -> list[ws]:
    result = await get_sample_async(db, opcd=opcd)
    if not result:
        return []
    return result

def sample_by_opcd_status(db: Session, opcd: str, status: str) -> list[ws]:
    result = get_sample(db, opcd=opcd, status=status)
    if not result:
        return []
    return result

async def sample_by_opcd_status_async(db: AsyncSession, opcd: str,status: str) -> list[ws]:
    result = await get_sample_async(db, opcd=opcd, status=status)
    if not result:
        return []
    return result