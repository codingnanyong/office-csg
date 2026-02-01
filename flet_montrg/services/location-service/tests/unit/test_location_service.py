"""
Unit tests for LocationService
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.services.location_service import LocationService
from app.models.database_models import Location as LocationModel, Sensor
from app.models.schemas import Location


class TestLocationService:
    """LocationService unit tests"""
    
    @pytest.fixture
    def mock_session(self):
        """Mock database session"""
        session = AsyncMock(spec=AsyncSession)
        return session
    
    @pytest.fixture
    def location_service(self, mock_session):
        """LocationService instance with mock session"""
        return LocationService(mock_session)
    
    @pytest.mark.asyncio
    async def test_get_all_locations_success(self, location_service, mock_session):
        """Test successful retrieval of all locations"""
        # Mock data
        mock_rows = [
            MagicMock(
                sensor_id="TEST001",
                loc_id="LOC001",
                factory="Test Factory",
                building="Test Building",
                floor=1,
                area="Test Area"
            ),
            MagicMock(
                sensor_id="TEST002",
                loc_id="LOC002",
                factory="Test Factory 2",
                building="Test Building 2",
                floor=2,
                area="Test Area 2"
            )
        ]
        
        # Mock execute and fetchall
        mock_result = MagicMock()
        mock_result.fetchall.return_value = mock_rows
        mock_session.execute.return_value = mock_result
        
        # Execute
        result = await location_service.get_all_locations(skip=0, limit=10)
        
        # Assert
        assert len(result) == 2
        assert result[0].sensor_id == "TEST001"
        assert result[0].loc_id == "LOC001"
        assert result[1].sensor_id == "TEST002"
        assert result[1].loc_id == "LOC002"
        
        # Verify query was executed
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_location_by_sensor_id_success(self, location_service, mock_session):
        """Test successful retrieval of location by sensor ID"""
        # Mock data
        mock_row = MagicMock(
            sensor_id="TEST001",
            loc_id="LOC001",
            factory="Test Factory",
            building="Test Building",
            floor=1,
            area="Test Area"
        )
        
        # Mock execute and fetchone
        mock_result = MagicMock()
        mock_result.fetchone.return_value = mock_row
        mock_session.execute.return_value = mock_result
        
        # Execute
        result = await location_service.get_location_by_sensor_id("TEST001")
        
        # Assert
        assert result is not None
        assert result.sensor_id == "TEST001"
        assert result.loc_id == "LOC001"
        assert result.factory == "Test Factory"
        
        # Verify query was executed
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_location_by_sensor_id_not_found(self, location_service, mock_session):
        """Test location not found by sensor ID"""
        # Mock empty result
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_session.execute.return_value = mock_result
        
        # Execute
        result = await location_service.get_location_by_sensor_id("NONEXISTENT")
        
        # Assert
        assert result is None
        
        # Verify query was executed
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_locations_by_sensor_ids_success(self, location_service, mock_session):
        """Test successful retrieval of locations by multiple sensor IDs"""
        # Mock data
        mock_rows = [
            MagicMock(
                sensor_id="TEST001",
                loc_id="LOC001",
                factory="Test Factory",
                building="Test Building",
                floor=1,
                area="Test Area"
            ),
            MagicMock(
                sensor_id="TEST002",
                loc_id="LOC002",
                factory="Test Factory 2",
                building="Test Building 2",
                floor=2,
                area="Test Area 2"
            )
        ]
        
        # Mock execute and fetchall
        mock_result = MagicMock()
        mock_result.fetchall.return_value = mock_rows
        mock_session.execute.return_value = mock_result
        
        # Execute
        sensor_ids = ["TEST001", "TEST002"]
        result = await location_service.get_locations_by_sensor_ids(sensor_ids)
        
        # Assert
        assert len(result) == 2
        assert result[0].sensor_id == "TEST001"
        assert result[1].sensor_id == "TEST002"
        
        # Verify query was executed
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_locations_by_sensor_ids_empty_list(self, location_service, mock_session):
        """Test retrieval with empty sensor IDs list"""
        # Execute
        result = await location_service.get_locations_by_sensor_ids([])
        
        # Assert
        assert result == []
        
        # Verify no query was executed
        mock_session.execute.assert_not_called()
