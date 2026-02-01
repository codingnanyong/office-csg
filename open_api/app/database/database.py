from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config.config import db_config
import os

Base = declarative_base()

ENV = os.getenv("ENV", "dev")

sync_engines = {
    "hq": create_engine(
        db_config.DATABASE_URLS["hq"].replace("postgresql+asyncpg", "postgresql"),
        future=True,
        echo=(ENV == "dev")
    ),
    "vj": create_engine(
        db_config.DATABASE_URLS["vj"].replace("postgresql+asyncpg", "postgresql"),
        future=True,
        echo=(ENV == "dev")
    )
}

async_engines = {
    "hq": create_async_engine(
        db_config.DATABASE_URLS["hq"],
        future=True,
        echo=(ENV == "dev")
    ),
    "vj": create_async_engine(
        db_config.DATABASE_URLS["vj"],
        future=True,
        echo=(ENV == "dev")
    )
}

SessionLocal = {
    key: sessionmaker(bind=sync_engines[key], autoflush=False, autocommit=False)
    for key in sync_engines.keys()
}

AsyncSessionLocal = {
    key: sessionmaker(bind=async_engines[key], class_=AsyncSession, expire_on_commit=False)
    for key in async_engines.keys()
}

def get_db(db_name="hq"):
    def _get_db():
        db = SessionLocal[db_name]()
        try:
            yield db
        finally:
            db.close()
    return _get_db

def get_async_db(db_name="hq"):
    async def _get_async_db():
        async with AsyncSessionLocal[db_name]() as session:
            yield session
    return _get_async_db
