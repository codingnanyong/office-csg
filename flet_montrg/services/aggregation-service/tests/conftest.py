"""
Pytest configuration and fixtures
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.main import app
from app.core.database import get_db
from app.models.database_models import Base
from app.models.schemas import AggregationRequest, TemperatureAggregationResponse, LocationData, HourlyData


# Test database setup
SQLALCHEMY_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

engine = create_async_engine(
    SQLALCHEMY_DATABASE_URL,
    poolclass=StaticPool,
    connect_args={"check_same_thread": False},
)

TestingSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    class_=AsyncSession,
)


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def db_session():
    """Create a test database session."""
    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async with TestingSessionLocal() as session:
        yield session
    
    # Drop tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
def client(db_session):
    """Create a test client with database dependency override."""
    def override_get_db():
        return db_session
    
    app.dependency_overrides[get_db] = override_get_db
    
    with TestClient(app) as test_client:
        yield test_client
    
    app.dependency_overrides.clear()


@pytest.fixture
def mock_aggregation_request():
    """Create a mock aggregation request."""
    return AggregationRequest(
        location_id="A031",
        start_date="20240922",
        end_date="20240922",
        start_hour="00",
        end_hour="23",
        metrics=["pcv_temperature_max", "pcv_temperature_avg"]
    )


@pytest.fixture
def mock_temperature_response():
    """Create a mock temperature aggregation response."""
    return TemperatureAggregationResponse(
        locations=[
            LocationData(
                factory="SinPyeong",
                building="F-2001",
                floor=1,
                loc_id="A031",
                area="조립2",
                date=[
                    HourlyData(
                        ymd="20240922",
                        hour="12",
                        metrics={
                            "pcv_temperature_max": "27.00",
                            "pcv_temperature_avg": "27.00"
                        }
                    )
                ]
            )
        ]
    )


@pytest.fixture
def mock_db_rows():
    """Create mock database rows."""
    class MockRow:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)
    
    return [
        MockRow(
            ymd="20240922",
            hour="12",
            factory="SinPyeong",
            building="F-2001",
            floor=1,
            area="조립2",
            loc_id="A031",
            pcv_temperature_max=27.00,
            pcv_temperature_min=26.50,
            pcv_temperature_avg=27.00,
            temperature_max=25.00,
            temperature_min=24.50,
            temperature_avg=24.75,
            humidity_max=65.00,
            humidity_min=60.00,
            humidity_avg=62.50
        ),
        MockRow(
            ymd="20240922",
            hour="13",
            factory="SinPyeong",
            building="F-2001",
            floor=1,
            area="조립2",
            loc_id="A031",
            pcv_temperature_max=27.50,
            pcv_temperature_min=27.00,
            pcv_temperature_avg=27.25,
            temperature_max=25.50,
            temperature_min=25.00,
            temperature_avg=25.25,
            humidity_max=67.00,
            humidity_min=62.00,
            humidity_avg=64.50
        )
    ]


@pytest.fixture
def mock_aggregation_service():
    """Create a mock aggregation service."""
    service = MagicMock()
    service.get_temperature_aggregation = AsyncMock()
    service.create_aggregation = AsyncMock()
    service.get_aggregations = AsyncMock()
    service.get_aggregation_by_id = AsyncMock()
    return service
