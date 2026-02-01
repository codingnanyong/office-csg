"""
Integration tests for main application endpoints
"""
import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from fastapi.testclient import TestClient
from datetime import datetime

from app.main import app
from app.models.schemas import TemperatureAggregationResponse, LocationData, HourlyData
from app.core.database import get_db


class TestBasicEndpoints:
    """Test basic application endpoints"""
    
    def test_root_endpoint(self, client):
        """Test root endpoint"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "docs" in data
        assert "environment" in data
        assert "version" in data
    
    def test_health_check(self, client):
        """Test health check endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}
    
    def test_readiness_check(self, client):
        """Test readiness check endpoint"""
        response = client.get("/ready")
        assert response.status_code == 200
        assert response.json() == {"status": "ready"}


class TestPcvTemperatureEndpoints:
    """Test PCV temperature aggregation endpoints"""
    
    @patch('app.api.v1.endpoints.aggregation.AggregationService')
    def test_get_all_pcv_temperature_success(self, mock_service_class, client):
        """Test successful GET all PCV temperature data"""
        # Mock the service and its methods
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_response = TemperatureAggregationResponse(
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
                            metrics={"pcv_temperature_max": "27.0", "pcv_temperature_avg": "27.0"}
                        )
                    ]
                )
            ]
        )
        mock_service.get_temperature_aggregation.return_value = mock_response
        
        response = client.get(
            "/api/v1/aggregation/pcv_temperature/",
            params={
                "start_date": "20240922",
                "end_date": "20240922"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["locations"]) >= 1
        assert data["locations"][0]["factory"] == "SinPyeong"
        assert data["locations"][0]["loc_id"] == "A031"
    
    @patch('app.api.v1.endpoints.aggregation.AggregationService')
    def test_get_all_pcv_temperature_no_data(self, mock_service_class, client):
        """Test GET all PCV temperature data with no data found"""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_temperature_aggregation.side_effect = ValueError("No data found for the given criteria")
        
        response = client.get(
            "/api/v1/aggregation/pcv_temperature/",
            params={
                "start_date": "20240922",
                "end_date": "20240922"
            }
        )
        
        assert response.status_code == 404
        assert "No data found" in response.json()["detail"]
    
    @patch('app.api.v1.endpoints.aggregation.AggregationService')
    def test_get_all_pcv_temperature_internal_error(self, mock_service_class, client):
        """Test GET all PCV temperature data with internal error"""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_temperature_aggregation.side_effect = Exception("Database connection failed")
        
        response = client.get(
            "/api/v1/aggregation/pcv_temperature/",
            params={
                "start_date": "20240922",
                "end_date": "20240922"
            }
        )
        
        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]
    
    @patch('app.api.v1.endpoints.aggregation.AggregationService')
    def test_get_pcv_temperature_by_location_success(self, mock_service_class, client):
        """Test successful GET PCV temperature by location"""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_response = TemperatureAggregationResponse(
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
                            metrics={"pcv_temperature_max": "27.0", "pcv_temperature_avg": "27.0"}
                        )
                    ]
                )
            ]
        )
        mock_service.get_temperature_aggregation.return_value = mock_response
        
        response = client.get(
            "/api/v1/aggregation/pcv_temperature/location/A031",
            params={
                "start_date": "20240922",
                "end_date": "20240922"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["locations"]) == 1
        assert data["locations"][0]["loc_id"] == "A031"
    
    @patch('app.api.v1.endpoints.aggregation.AggregationService')
    def test_get_pcv_temperature_by_factory_success(self, mock_service_class, client):
        """Test successful GET PCV temperature by factory"""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_response = TemperatureAggregationResponse(
            locations=[
                LocationData(
                    factory="SinPyeong",
                    building=None,
                    floor=None,
                    loc_id=None,
                    area=None,
                    date=[
                        HourlyData(
                            ymd="20240922",
                            hour="12",
                            metrics={"pcv_temperature_max": "27.0", "pcv_temperature_avg": "27.0"}
                        )
                    ]
                )
            ]
        )
        mock_service.get_temperature_aggregation.return_value = mock_response
        
        response = client.get(
            "/api/v1/aggregation/pcv_temperature/factory/SinPyeong",
            params={
                "start_date": "20240922",
                "end_date": "20240922"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["locations"]) == 1
        assert data["locations"][0]["factory"] == "SinPyeong"
        assert data["locations"][0]["building"] is None
    
    def test_get_pcv_temperature_missing_required_params(self, client):
        """Test GET PCV temperature with missing required parameters"""
        response = client.get("/api/v1/aggregation/pcv_temperature/")
        
        assert response.status_code == 422  # Validation error
    
    @patch('app.api.v1.endpoints.aggregation.AggregationService')
    def test_get_pcv_temperature_dynamic_date_formats(self, mock_service_class, client):
        """Test GET PCV temperature with different date formats"""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_response = TemperatureAggregationResponse(
            locations=[
                LocationData(
                    factory="SinPyeong",
                    building="F-2001",
                    floor=1,
                    loc_id="A031",
                    area="조립2",
                    date=[]
                )
            ]
        )
        mock_service.get_temperature_aggregation.return_value = mock_response
        
        # Test year format
        response = client.get(
            "/api/v1/aggregation/pcv_temperature/",
            params={"start_date": "2024", "end_date": "2024"}
        )
        assert response.status_code == 200
        
        # Test year-month format
        response = client.get(
            "/api/v1/aggregation/pcv_temperature/",
            params={"start_date": "202409", "end_date": "202409"}
        )
        assert response.status_code == 200
        
        # Test year-month-day format
        response = client.get(
            "/api/v1/aggregation/pcv_temperature/",
            params={"start_date": "20240922", "end_date": "20240922"}
        )
        assert response.status_code == 200


class TestLegacyEndpoints:
    """Test that legacy endpoints return 404"""
    
    def test_legacy_temperature_endpoints_not_found(self, client):
        """Test that old temperature endpoints return 404"""
        # Test old POST endpoint
        response = client.post("/api/v1/aggregation/temperature")
        assert response.status_code == 404
        
        # Test old GET endpoints
        response = client.get("/api/v1/aggregation/temperature/location/A031")
        assert response.status_code == 404
        
        response = client.get("/api/v1/aggregation/temperature/factory/SinPyeong")
        assert response.status_code == 404