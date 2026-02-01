"""
Pytest configuration and fixtures for Alert Subscription Service
"""
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, BigInteger, Integer, String, Boolean, DateTime

# 테스트용 Base 생성
TestBase = declarative_base()

# 테스트용 모델 (스키마 없음, SQLite 호환)
class TestAlertSubscription(TestBase):
    """테스트용 AlertSubscription 모델 (SQLite 호환)"""
    __tablename__ = "alert_subscriptions"
    __table_args__ = {}  # 스키마 없음
    
    # SQLite에서는 Integer를 사용해야 autoincrement가 제대로 작동
    subscription_id = Column(Integer, primary_key=True, autoincrement=True)
    plant = Column(String(50), nullable=True)
    factory = Column(String(50), nullable=True)
    building = Column(String(50), nullable=True)
    floor = Column(Integer, nullable=True)
    area = Column(String(50), nullable=True)
    sensor_id = Column(String(50), nullable=True, index=True)
    threshold_type = Column(String(50), nullable=True)
    min_level = Column(String(20), nullable=True)
    subscriber = Column(String(100), nullable=False, index=True)
    notify_type = Column(String(20), nullable=False, default="email")
    notify_id = Column(String(200), nullable=False)
    enabled = Column(Boolean, nullable=False, default=True, index=True)
    upd_dt = Column(DateTime, nullable=True)  # SQLite 호환을 위해 nullable=True로 변경, Python에서 설정

# 모델 교체를 먼저 수행 (app import 전에)
import app.models.database_models as db_models
_original_subscription = db_models.AlertSubscription
db_models.AlertSubscription = TestAlertSubscription

# 이제 app을 import (모델이 이미 교체된 상태)
from app.main import app
from app.core.database import Base, get_db

# 서비스 모듈에서도 모델 교체
import app.services.subscription_service as subscription_service_module
if hasattr(subscription_service_module, 'AlertSubscription'):
    subscription_service_module.AlertSubscription = TestAlertSubscription

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
    from datetime import datetime
    
    # 기존 테이블 삭제
    async with engine.begin() as conn:
        await conn.run_sync(TestBase.metadata.drop_all)
    
    # 테이블 생성 (TestAlertSubscription 모델 사용)
    async with engine.begin() as conn:
        await conn.run_sync(TestBase.metadata.create_all)
    
    yield
    
    # 테스트 후 정리
    async with engine.begin() as conn:
        await conn.run_sync(TestBase.metadata.drop_all)
    
    # 원본 모델 복원 (다른 테스트나 실제 앱에 영향 주지 않도록)
    db_models.AlertSubscription = _original_subscription


@pytest.fixture
def sample_subscription_data():
    """샘플 구독 데이터 fixture"""
    return {
        "plant": "Plant1",
        "factory": "Factory1",
        "building": "Building1",
        "floor": 1,
        "area": "Area1",
        "sensor_id": "SENSOR001",
        "threshold_type": "temperature",
        "min_level": "high",
        "subscriber": "test_user",
        "notify_type": "email",
        "notify_id": "test@example.com",
        "enabled": True
    }


pytestmark = pytest.mark.asyncio
