"""
Pytest configuration and fixtures for Alert Notification Service
"""
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, BigInteger, Integer, String, DateTime, Text

# 테스트용 Base 생성
TestBase = declarative_base()

# 테스트용 모델 (스키마 없음, SQLite 호환)
class TestAlertNotification(TestBase):
    """테스트용 AlertNotification 모델 (SQLite 호환)"""
    __tablename__ = "alert_notifications"
    __table_args__ = {}  # 스키마 없음
    
    # SQLite에서는 Integer를 사용해야 autoincrement가 제대로 작동
    notification_id = Column(Integer, primary_key=True, autoincrement=True)
    alert_id = Column(Integer, nullable=False, index=True)
    subscription_id = Column(Integer, nullable=False, index=True)
    notify_type = Column(String(20), nullable=False)
    notify_id = Column(String(200), nullable=False)
    status = Column(String(20), nullable=False, default="PENDING", index=True)
    try_count = Column(Integer, nullable=False, default=0)
    created_time = Column(DateTime, nullable=True)  # SQLite 호환을 위해 nullable=True로 변경, Python에서 설정
    last_try_time = Column(DateTime, nullable=True, index=True)
    sent_time = Column(DateTime, nullable=True)
    fail_reason = Column(Text, nullable=True)

# 모델 교체를 먼저 수행 (app import 전에)
import app.models.database_models as db_models
_original_notification = db_models.AlertNotification
db_models.AlertNotification = TestAlertNotification

# 이제 app을 import (모델이 이미 교체된 상태)
from app.main import app
from app.core.database import Base, get_db

# 서비스 모듈에서도 모델 교체
import app.services.notification_service as notification_service_module
if hasattr(notification_service_module, 'AlertNotification'):
    notification_service_module.AlertNotification = TestAlertNotification

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
    
    # 테이블 생성 (TestAlertNotification 모델 사용)
    async with engine.begin() as conn:
        await conn.run_sync(TestBase.metadata.create_all)
    
    yield
    
    # 테스트 후 정리
    async with engine.begin() as conn:
        await conn.run_sync(TestBase.metadata.drop_all)
    
    # 원본 모델 복원 (다른 테스트나 실제 앱에 영향 주지 않도록)
    db_models.AlertNotification = _original_notification


@pytest.fixture
def sample_notification_data():
    """샘플 알림 데이터 fixture"""
    return {
        "alert_id": 1,
        "subscription_id": 1,
        "notify_type": "email",
        "notify_id": "test@example.com"
    }


pytestmark = pytest.mark.asyncio
