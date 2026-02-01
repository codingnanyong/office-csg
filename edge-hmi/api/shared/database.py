"""SQLAlchemy engine and session."""
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from shared.config import settings

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    echo=False,
    connect_args={"options": f"-c search_path={settings.POSTGRES_SCHEMA},public"},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
