"""
Pytest configuration and fixtures
"""
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, DateTime, Integer, Boolean

# 테스트용 Base 생성
TestBase = declarative_base()

# 테스트용 모델 (스키마 없음, SQLite 호환)
class TestLocation(TestBase):
    """테스트용 Location 모델 (SQLite 호환)"""
    __tablename__ = "locations"
    __table_args__ = {}  # 스키마 없음

    loc_id = Column(String(10), primary_key=True, index=True)
    plant = Column(String(50), nullable=True)
    factory = Column(String(50), nullable=True)
    building = Column(String(50), nullable=True)
    floor = Column(Integer, nullable=True)
    area = Column(String(50), nullable=True)
    location_name = Column(String(200), nullable=True)
    upd_dt = Column(DateTime, nullable=True)
    is_active = Column(Boolean, nullable=True, default=True)


class TestSensor(TestBase):
    """테스트용 Sensor 모델 (SQLite 호환)"""
    __tablename__ = "sensors"
    __table_args__ = {}  # 스키마 없음

    sensor_id = Column(String(50), primary_key=True, index=True)
    loc_id = Column(String(10), nullable=True)
    name = Column(String(100), nullable=True)
    upd_dt = Column(DateTime, nullable=True)

# 모델 교체를 먼저 수행 (app import 전에)
import app.models.database_models as db_models
_original_location_model = db_models.Location
_original_sensor_model = db_models.Sensor
db_models.Location = TestLocation
db_models.Sensor = TestSensor

# 이제 app을 import (모델이 이미 교체된 상태)
from app.main import app
from app.core.database import Base, get_db

# 서비스 모듈에서도 모델 교체
import app.services.location_service as location_service_module
location_service_module.LocationModel = TestLocation
location_service_module.Sensor = TestSensor

# 테스트용 데이터베이스 설정
SQLALCHEMY_DATABASE_URL = "sqlite+aiosqlite:///:memory:"
engine = create_async_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    echo=False
)
TestingSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def override_get_db():
    """테스트용 데이터베이스 세션"""
    async with TestingSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

# 의존성 오버라이드
app.dependency_overrides[get_db] = override_get_db

@pytest_asyncio.fixture
async def client():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac

@pytest_asyncio.fixture(autouse=True)
async def setup_db():
    """각 테스트 전에 실행되는 설정 - 테이블 생성 및 삭제"""
    # 기존 테이블 삭제
    async with engine.begin() as conn:
        await conn.run_sync(TestBase.metadata.drop_all)
    
    # 테이블 생성 (TestLocation, TestSensor 모델 사용)
    async with engine.begin() as conn:
        await conn.run_sync(TestBase.metadata.create_all)
    
    yield
    
    # 테스트 후 정리
    async with engine.begin() as conn:
        await conn.run_sync(TestBase.metadata.drop_all)
    
    # 원본 모델 복원 (다른 테스트나 실제 앱에 영향 주지 않도록)
    db_models.Location = _original_location_model
    db_models.Sensor = _original_sensor_model

pytestmark = pytest.mark.asyncio
