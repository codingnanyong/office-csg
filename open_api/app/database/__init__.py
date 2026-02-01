from .database import sync_engines, async_engines, get_db, get_async_db, Base

sync_engine = sync_engines["hq"]
async_engine = async_engines["hq"]