from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.helpers.tag_current_helper import get_tag_data,get_tag_data_async
from app.services.helpers.tag_history_helper import get_tag_history_data,get_tag_history_data_async

from app.schemas.hq.tag_current_schema import CurrentTag
from app.schemas.hq.tag_history_schema import TagHistory

def tags(db: Session) -> list[CurrentTag]:
    result = get_tag_data(db)
    if not result:
        return []
    return result

async def tags_async(db: AsyncSession) -> list[CurrentTag]:
    result = await get_tag_data_async(db)
    if not result:
        return []
    return result

def tag_by_ws(db: Session,wsno: str = None) -> CurrentTag:
    result = get_tag_data(db,wsno=wsno)
    if not result:
        return []
    return result

async def tag_by_ws_async(db: AsyncSession,wsno: str = None) -> CurrentTag:
    result = await get_tag_data_async(db,wsno=wsno)
    if not result:
        return []
    return result

def tag_histories_by_ws(db: Session,wsno: str = None) -> TagHistory:
    result = get_tag_history_data(db,wsno=wsno)
    if not result:
        return []
    return result

async def tag_histories_by_ws_async(db: AsyncSession,wsno: str = None) -> TagHistory:
    result = await get_tag_history_data_async(db,wsno=wsno)
    if not result:
        return []
    return result