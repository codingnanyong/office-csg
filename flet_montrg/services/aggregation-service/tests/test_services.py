"""
Unit tests for services
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from decimal import Decimal

from app.services.aggregation_service import AggregationService
from app.models.schemas import AggregationRequest, TemperatureAggregationResponse, LocationData, HourlyData


class TestAggregationService:
    """Test AggregationService class"""
    
    @pytest.fixture
    def mock_db_session(self):
        """Create a mock database session"""
        session = AsyncMock()
        session.execute = AsyncMock()
        session.flush = AsyncMock()
        session.add = MagicMock()
        session.commit = AsyncMock()
        session.rollback = AsyncMock()
        return session
    
    @pytest.fixture
    def aggregation_service(self, mock_db_session):
        """Create AggregationService instance with mock database"""
        return AggregationService(mock_db_session)
    
    @pytest.fixture
    def mock_request(self):
        """Create a mock aggregation request"""
        return AggregationRequest(
            location_id="A031",
            start_date="20240922",
            end_date="20240922",
            start_hour="00",
            end_hour="23",
            metrics=["pcv_temperature_max", "pcv_temperature_avg"]
        )
    
    @pytest.fixture
    def mock_request_factory(self):
        """Create a mock factory-level aggregation request"""
        return AggregationRequest(
            factory="SinPyeong",
            start_date="20240922",
            end_date="20240922",
            start_hour="00",
            end_hour="23",
            metrics=["pcv_temperature_max", "pcv_temperature_avg"]
        )
    
    @pytest.fixture
    def mock_request_building(self):
        """Create a mock building-level aggregation request"""
        return AggregationRequest(
            factory="SinPyeong",
            building="F-2001",
            start_date="20240922",
            end_date="20240922",
            start_hour="00",
            end_hour="23",
            metrics=["pcv_temperature_max", "pcv_temperature_avg"]
        )
    
    @pytest.fixture
    def mock_request_floor(self):
        """Create a mock floor-level aggregation request"""
        return AggregationRequest(
            factory="SinPyeong",
            building="F-2001",
            floor=1,
            start_date="20240922",
            end_date="20240922",
            start_hour="00",
            end_hour="23",
            metrics=["pcv_temperature_max", "pcv_temperature_avg"]
        )

    # Validation tests
    def test_validate_date_format_valid(self, aggregation_service):
        """Test valid date format validation"""
        # Should not raise any exception
        aggregation_service._validate_date_format("2024", "년도")
        aggregation_service._validate_date_format("202409", "년월")
        aggregation_service._validate_date_format("20240922", "년월일")
    
    def test_validate_date_format_invalid(self, aggregation_service):
        """Test invalid date format validation"""
        with pytest.raises(ValueError, match="형식이어야 합니다"):
            aggregation_service._validate_date_format("24", "년도")
        
        with pytest.raises(ValueError, match="유효하지 않은"):
            aggregation_service._validate_date_format("202413", "년월")  # Invalid month
        
        with pytest.raises(ValueError, match="형식이어야 합니다"):
            aggregation_service._validate_date_format("20240932", "년월일")  # Invalid day
    
    def test_validate_date_range_valid(self, aggregation_service):
        """Test valid date range validation"""
        # Should not raise any exception
        aggregation_service._validate_date_range("202401", "202412")
        aggregation_service._validate_date_range("20240922", "20240922")
    
    def test_validate_date_range_invalid(self, aggregation_service):
        """Test invalid date range validation"""
        with pytest.raises(ValueError, match="늦을 수 없습니다"):
            aggregation_service._validate_date_range("202412", "202401")
    
    def test_validate_hour_format_valid(self, aggregation_service):
        """Test valid hour format validation"""
        # Should not raise any exception
        aggregation_service._validate_hour_format("00", "시작 시간")
        aggregation_service._validate_hour_format("23", "종료 시간")
        aggregation_service._validate_hour_format("12", "시간")
        aggregation_service._validate_hour_format(None, "시간")  # None should be allowed
    
    def test_validate_hour_format_invalid(self, aggregation_service):
        """Test invalid hour format validation"""
        with pytest.raises(ValueError, match="00-23 형식이어야 합니다"):
            aggregation_service._validate_hour_format("24", "시간")
        
        with pytest.raises(ValueError, match="00-23 형식이어야 합니다"):
            aggregation_service._validate_hour_format("25", "시간")
    
    def test_validate_hour_range_valid(self, aggregation_service):
        """Test valid hour range validation"""
        # Should not raise any exception
        aggregation_service._validate_hour_range("00", "23")
        aggregation_service._validate_hour_range("12", "12")
        aggregation_service._validate_hour_range(None, "23")
        aggregation_service._validate_hour_range("00", None)
        aggregation_service._validate_hour_range(None, None)
    
    def test_validate_hour_range_invalid(self, aggregation_service):
        """Test invalid hour range validation"""
        with pytest.raises(ValueError, match="늦을 수 없습니다"):
            aggregation_service._validate_hour_range("23", "00")
    
    def test_validate_metrics_valid(self, aggregation_service):
        """Test valid metrics validation"""
        # Should not raise any exception
        aggregation_service._validate_metrics(["pcv_temperature_max"])
        aggregation_service._validate_metrics(["pcv_temperature_avg"])
        aggregation_service._validate_metrics(["temperature_max"])
        aggregation_service._validate_metrics(["temperature_avg"])
        aggregation_service._validate_metrics(["humidity_max"])
        aggregation_service._validate_metrics(["humidity_avg"])
        aggregation_service._validate_metrics(["pcv_temperature_max", "pcv_temperature_avg", "temperature_max", "temperature_avg", "humidity_max", "humidity_avg"])
    
    def test_validate_metrics_invalid(self, aggregation_service):
        """Test invalid metrics validation"""
        with pytest.raises(ValueError, match="비어있습니다"):
            aggregation_service._validate_metrics([])
        
        with pytest.raises(ValueError, match="유효하지 않은 메트릭입니다"):
            aggregation_service._validate_metrics(["invalid_metric"])
    
    def test_validate_request_valid(self, aggregation_service, mock_request):
        """Test valid request validation"""
        # Should not raise any exception
        aggregation_service._validate_request(mock_request)
    
    def test_validate_request_invalid_date_range(self, aggregation_service):
        """Test invalid request with wrong date range"""
        invalid_request = AggregationRequest(
            location_id="A031",
            start_date="202412",
            end_date="202401",  # End date before start date
            metrics=["pcv_temperature_max"]
        )
        
        with pytest.raises(ValueError, match="늦을 수 없습니다"):
            aggregation_service._validate_request(invalid_request)
    
    def test_validate_request_invalid_metrics(self, aggregation_service):
        """Test invalid request with wrong metrics"""
        invalid_request = AggregationRequest(
            location_id="A031",
            start_date="20240922",
            end_date="20240922",
            metrics=["invalid_metric"]
        )
        
        with pytest.raises(ValueError, match="유효하지 않은 메트릭입니다"):
            aggregation_service._validate_request(invalid_request)
    
    # Aggregation level tests
    def test_determine_aggregation_level_factory(self, aggregation_service, mock_request_factory):
        """Test factory level aggregation determination"""
        level = aggregation_service._determine_aggregation_level(mock_request_factory)
        assert level == "factory"
    
    def test_determine_aggregation_level_building(self, aggregation_service, mock_request_building):
        """Test building level aggregation determination"""
        level = aggregation_service._determine_aggregation_level(mock_request_building)
        assert level == "building"
    
    def test_determine_aggregation_level_floor(self, aggregation_service, mock_request_floor):
        """Test floor level aggregation determination"""
        level = aggregation_service._determine_aggregation_level(mock_request_floor)
        assert level == "floor"
    
    def test_determine_aggregation_level_location(self, aggregation_service, mock_request):
        """Test location level aggregation determination"""
        level = aggregation_service._determine_aggregation_level(mock_request)
        assert level == "location"
    
    @pytest.mark.skip(reason="Method _extract_location_info removed from service")
    def test_extract_location_info_factory(self, aggregation_service, mock_db_rows):
        """Test location info extraction for factory level"""
        pass
    
    @pytest.mark.skip(reason="Method _extract_location_info removed from service")
    def test_extract_location_info_building(self, aggregation_service, mock_db_rows):
        """Test location info extraction for building level"""
        pass
    
    @pytest.mark.skip(reason="Method _extract_location_info removed from service")
    def test_extract_location_info_floor(self, aggregation_service, mock_db_rows):
        """Test location info extraction for floor level"""
        pass
    
    @pytest.mark.skip(reason="Method _extract_location_info removed from service")
    def test_extract_location_info_location(self, aggregation_service, mock_db_rows):
        """Test location info extraction for location level"""
        pass

    @pytest.mark.asyncio
    async def test_structure_temperature_data(self, aggregation_service, mock_db_rows):
        """Test temperature data structuring"""
        # Mock the database execution
        mock_result = MagicMock()
        mock_result.fetchall.return_value = mock_db_rows
        aggregation_service.db.execute.return_value = mock_result

        # Test data structuring
        result = await aggregation_service._structure_temperature_data(mock_db_rows, ["pcv_temperature_max", "pcv_temperature_avg"])

        assert isinstance(result, TemperatureAggregationResponse)
        assert result.locations[0].factory == "SinPyeong"
        assert result.locations[0].building == "F-2001"
        assert result.locations[0].loc_id == "A031"
        assert len(result.locations[0].date) == 2

        # Check first hourly data
        first_hourly = result.locations[0].date[0]
        assert first_hourly.ymd == "20240922"
        assert first_hourly.hour == "12"  # Sorted in ASC order
        assert "pcv_temperature_max" in first_hourly.metrics
        assert "pcv_temperature_avg" in first_hourly.metrics
        assert first_hourly.metrics["pcv_temperature_max"] == "27.0"

    @pytest.mark.asyncio
    async def test_get_temperature_aggregation_success(self, aggregation_service, mock_request, mock_db_rows):
        """Test successful temperature aggregation"""
        # Mock hour check query - returns count > 0
        hour_check_result = MagicMock()
        hour_check_result.fetchone.return_value.count = 2
        
        # Mock main query result
        main_result = MagicMock()
        main_result.fetchall.return_value = mock_db_rows
        
        # Setup mock to return different results for different queries
        async def mock_execute(query, params=None):
            query_str = str(query)
            if "COUNT(*)" in query_str:
                return hour_check_result
            else:
                return main_result
        
        aggregation_service.db.execute.side_effect = mock_execute
        
        expected_response = TemperatureAggregationResponse(
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
                                "pcv_temperature_max": "27.0",
                                "pcv_temperature_avg": "27.0"
                            }
                        )
                    ]
                )
            ]
        )
        
        with patch.object(aggregation_service, '_structure_temperature_data', return_value=expected_response):
            result = await aggregation_service.get_temperature_aggregation(mock_request)
            
            assert isinstance(result, TemperatureAggregationResponse)
            assert result.locations[0].loc_id == "A031"
            assert aggregation_service.db.execute.called

    @pytest.mark.asyncio
    async def test_get_temperature_aggregation_no_data(self, aggregation_service, mock_request):
        """Test temperature aggregation with no data"""
        # Mock hour check query - returns count > 0
        hour_check_result = MagicMock()
        hour_check_result.fetchone.return_value.count = 2
        
        # Mock empty database result
        empty_result = MagicMock()
        empty_result.fetchall.return_value = []
        
        # Setup mock to return different results for different queries
        async def mock_execute(query, params=None):
            query_str = str(query)
            if "COUNT(*)" in query_str:
                return hour_check_result
            else:
                return empty_result
        
        aggregation_service.db.execute.side_effect = mock_execute
        
        with pytest.raises(ValueError, match="No data found for the given criteria"):
            await aggregation_service.get_temperature_aggregation(mock_request)

    @pytest.mark.asyncio
    async def test_get_temperature_aggregation_database_error(self, aggregation_service, mock_request):
        """Test temperature aggregation with database error"""
        # Mock database error
        aggregation_service.db.execute.side_effect = Exception("Database connection failed")
        
        with pytest.raises(Exception, match="Database connection failed"):
            await aggregation_service.get_temperature_aggregation(mock_request)

    # Removed create_aggregation path; aggregation persistence handled elsewhere (e.g., Airflow)

    # Note: get_aggregations and get_aggregation_by_id methods were removed from the service
    # These tests are kept for reference but will be skipped
    @pytest.mark.skip(reason="Method removed from service")
    @pytest.mark.asyncio
    async def test_get_aggregations(self, aggregation_service):
        """Test getting all aggregations"""
        pass

    @pytest.mark.skip(reason="Method removed from service")
    @pytest.mark.asyncio
    async def test_get_aggregation_by_id_success(self, aggregation_service):
        """Test getting aggregation by ID - success"""
        pass

    @pytest.mark.skip(reason="Method removed from service")
    @pytest.mark.asyncio
    async def test_get_aggregation_by_id_not_found(self, aggregation_service):
        """Test getting aggregation by ID - not found"""
        pass