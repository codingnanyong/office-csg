import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.database.database import SessionLocal, AsyncSessionLocal, get_db, get_async_db
import asyncio

@pytest.fixture(scope="module")
def client():
    with TestClient(app) as client:
        yield client

@pytest.fixture(scope="function")
def db_session():
    db = SessionLocal["hq"]()
    try:
        yield db  
    finally:
        db.close()

@pytest.fixture(scope="function")
async def async_db_session():
    async with AsyncSessionLocal["hq"]() as session:
        yield session

app.dependency_overrides[get_db] = lambda: next(db_session())
app.dependency_overrides[get_async_db] = lambda: async_db_session()
