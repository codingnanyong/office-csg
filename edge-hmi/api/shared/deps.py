"""FastAPI dependencies."""
from collections.abc import Generator

from sqlalchemy.orm import Session

from shared.database import SessionLocal


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
