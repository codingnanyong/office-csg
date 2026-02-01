"""
Database configuration and session management.
"""

import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, List

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

from app.core.config import settings

# Oracle thick mode Initialization (Oracle Instant Client required)
# Initialize only if CMMS Database exists
if "cmms" in settings.database_urls:
    try:
        import oracledb

        # Force using thick mode
        oracledb.defaults.thin_mode = False

        lib_path = os.environ.get("ORACLE_CLIENT_LIB", "/opt/oracle/instantclient_23_7")
        try:
            oracledb.init_oracle_client(lib_dir=lib_path)
        except oracledb.ProgrammingError as e:
            if "DPY-0005" in str(e):
                # Already initialized
                pass
            else:
                raise
    except Exception as e:
        # Oracle Instant Client initialization failed, so it's better to fail explicitly
        # (thin mode does not support older Oracle server versions)
        raise RuntimeError(
            f"Oracle thick mode initialization failed: {e}. "
            "Check that Oracle Instant Client is correctly installed and ORACLE_CLIENT_LIB is set."
        ) from e

if not settings.database_urls:
    raise RuntimeError("At least one database URL must be configured.")

Base = declarative_base()

# Manage both asynchronous and synchronous engines
# - Asynchronous engine: PostgreSQL, MySQL, etc.
# - Synchronous engine: Oracle(CMMS), etc. (thick mode required)
ASYNC_ENGINES: Dict[str, "AsyncEngine"] = {}
SYNC_ENGINES: Dict[str, Engine] = {}

for alias, url in settings.database_urls.items():
    # Common engine settings
    common_kwargs = {
        "echo": settings.debug,
        "pool_pre_ping": True,
        "pool_recycle": 3600,
    }

    # Oracle(CMMS) uses only synchronous engine (thick mode)
    if alias == "cmms" and url.lower().startswith("oracle+oracledb"):
        SYNC_ENGINES[alias] = create_engine(url, **common_kwargs)
    else:
        ASYNC_ENGINES[alias] = create_async_engine(url, **common_kwargs)

SESSION_FACTORIES: Dict[str, async_sessionmaker[AsyncSession]] = {
    alias: async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    for alias, engine in ASYNC_ENGINES.items()
}

DEFAULT_DB_ALIAS = settings.default_database_alias or next(iter(settings.database_urls.keys()))


def available_databases() -> List[str]:
    """Return the list of configured database aliases."""
    return list(SESSION_FACTORIES.keys())


def resolve_db_alias(alias: str | None) -> str:
    """Return a valid database alias, defaulting to the configured default."""
    resolved = alias or DEFAULT_DB_ALIAS
    if resolved not in settings.database_urls:
        raise ValueError(f"Unknown database alias '{resolved}'. Available: {available_databases()}")
    return resolved


def get_sync_engine(alias: str | None = None) -> Engine:
    """Return synchronous SQLAlchemy Engine (for Oracle CMMS).
    
    Oracle(CMMS) does not use asynchronous engine, only synchronous thick mode engine is used.
    """
    resolved = resolve_db_alias(alias)
    if resolved not in SYNC_ENGINES:
        raise ValueError(f"No synchronous engine configured for alias '{resolved}'")
    return SYNC_ENGINES[resolved]


@asynccontextmanager
async def get_session(db_alias: str | None = None) -> AsyncGenerator[AsyncSession, None]:
    """Provide an async DB session for the given alias."""
    alias = resolve_db_alias(db_alias)
    if alias not in SESSION_FACTORIES:
        raise ValueError(f"Asynchronous session is not configured for alias '{alias}'")
    session_factory = SESSION_FACTORIES[alias]
    async with session_factory() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db() -> None:
    """Initialize databases by creating tables where necessary."""
    for engine in ASYNC_ENGINES.values():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)