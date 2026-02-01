"""
Pytest configuration and fixtures for Alert Service
"""
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, BigInteger, Integer, String, Numeric, DateTime, Text

# 테스트용 Base 생성
TestBase = declarative_base()

# 테스트용 모델 (스키마 없음, SQLite 호환)
class TestAlert(TestBase):
    """테스트용 Alert 모델 (SQLite 호환)"""
    __tablename__ = "alerts"
    __table_args__ = {}  # 스키마 없음
    
    # SQLite에서는 Integer를 사용해야 autoincrement가 제대로 작동
    alert_id = Column(Integer, primary_key=True, autoincrement=True)
    alert_time = Column(DateTime, nullable=False)  # server_default 제거
    loc_id = Column(String(10), nullable=True)
    sensor_id = Column(String(50), nullable=False, index=True)
    alert_type = Column(String(50), nullable=False)
    alert_level = Column(String(20), nullable=False, index=True)
    threshold_id = Column(Integer, nullable=False, index=True)
    threshold_map_id = Column(BigInteger, nullable=True)
    threshold_type = Column(String(50), nullable=False, index=True)
    threshold_level = Column(String(20), nullable=False)
    measured_value = Column(Numeric(10, 3), nullable=True)
    threshold_min = Column(Numeric(10, 3), nullable=True)
    threshold_max = Column(Numeric(10, 3), nullable=True)
    message = Column(Text, nullable=True)

# 모델 교체를 먼저 수행 (app import 전에)
import app.models.database_models as db_models
_original_alert = db_models.Alert
db_models.Alert = TestAlert

# 이제 app을 import (모델이 이미 교체된 상태)
from app.main import app
from app.core.database import Base, get_db

# 서비스 모듈에서도 모델 교체
import app.services.alert_service as alert_service_module
if hasattr(alert_service_module, 'Alert'):
    alert_service_module.Alert = TestAlert

# 테스트용 데이터베이스 설정 (SQLite in-memory)
SQLALCHEMY_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

# Create async engine for testing
engine = create_async_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    echo=False
)

# Create async session maker for testing
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
    """테스트 클라이언트 fixture"""
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture(autouse=True)
async def setup_db():
    """각 테스트 전에 실행되는 설정 - 테이블 생성 및 삭제"""
    # 기존 테이블 삭제
    async with engine.begin() as conn:
        await conn.run_sync(TestBase.metadata.drop_all)
    
    # 테이블 생성 (TestAlert 모델 사용)
    async with engine.begin() as conn:
        await conn.run_sync(TestBase.metadata.create_all)
    
    yield
    
    # 테스트 후 정리
    async with engine.begin() as conn:
        await conn.run_sync(TestBase.metadata.drop_all)
    
    # 원본 모델 복원 (다른 테스트나 실제 앱에 영향 주지 않도록)
    db_models.Alert = _original_alert


@pytest.fixture
def sample_alert_data():
    """샘플 알람 데이터 fixture"""
    return {
        "sensor_id": "SENSOR001",
        "loc_id": "A031",
        "alert_type": "temperature",
        "alert_level": "high",
        "threshold_id": 1,
        "threshold_map_id": 1,
        "threshold_type": "temperature",
        "threshold_level": "high",
        "measured_value": 35.5,
        "threshold_min": 20.0,
        "threshold_max": 30.0,
        "message": "Temperature exceeded threshold"
    }


pytestmark = pytest.mark.asyncio
