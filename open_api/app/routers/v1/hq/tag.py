from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from app.database.database import get_db, get_async_db
from app.services.hq.tag_services import ( 
    tags,
    tags_async,
    tag_by_ws,
    tag_by_ws_async,
    tag_histories_by_ws,
    tag_histories_by_ws_async
)
from app.schemas.hq.tag_current_schema import CurrentTag
from app.schemas.hq.tag_history_schema import TagHistory

get_hq_db= get_db('hq')
get_hq_async_db = get_async_db('hq')

router = APIRouter()

# --- Current Tag Endpoints ---

@router.get("/current", response_model=list[CurrentTag])
def get_current_tags(db: Session = Depends(get_hq_db)):
    current_tags = tags(db)
    if not current_tags:
        raise HTTPException(status_code=404, detail="No Tags found")
    return current_tags

@router.get("/current/async", response_model=list[CurrentTag])
async def get_current_tags_async(db: AsyncSession = Depends(get_hq_async_db)):
    current_tags = await tags_async(db)
    if not current_tags:
        raise HTTPException(status_code=404, detail="No Tags found")
    return current_tags

@router.get("/current/{wsno}", response_model=CurrentTag)
def get_current_tag_by_ws(wsno: str, db: Session = Depends(get_hq_db)):
    current_tag = tag_by_ws(db, wsno)
    if not current_tag:
        raise HTTPException(status_code=404, detail="No Tag found")
    return current_tag

@router.get("/current/{wsno}/async", response_model=CurrentTag)
async def get_current_tag_by_ws_async(wsno: str, db: AsyncSession = Depends(get_hq_async_db)):
    current_tag = await tag_by_ws_async(db, wsno)
    if not current_tag:
        raise HTTPException(status_code=404, detail="No Tag found")
    return current_tag

# --- Tag History Endpoints ---

@router.get("/histories/ws", response_model=TagHistory)
def get_tag_history_by_ws(wsno: str = Query(...), db: Session = Depends(get_hq_db)):
    history = tag_histories_by_ws(db, wsno)
    if not history:
        raise HTTPException(status_code=404, detail="No Tag History found")
    return history

@router.get("/histories/ws/async", response_model=TagHistory)
async def get_tag_history_by_ws_async(wsno: str = Query(...), db: AsyncSession = Depends(get_hq_async_db)):
    history = await tag_histories_by_ws_async(db, wsno)
    if not history:
        raise HTTPException(status_code=404, detail="No Tag History found")
    return history