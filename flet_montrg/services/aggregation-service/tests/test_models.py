"""
Unit tests for models and schemas
"""
import pytest
from datetime import datetime
from decimal import Decimal

from app.models.schemas import (
    AggregationRequest, 
    TemperatureAggregationResponse,
    LocationData,
    HourlyData,
    MetricValue,
    AggregationStats,
    TimeSeriesData
)


class TestAggregationRequest:
    """Test AggregationRequest model"""
    
    def test_valid_request(self):
        """Test valid aggregation request"""
        request = AggregationRequest(
            location_id="A031",
            factory="SinPyeong",
            building="F-2001",
            area="조립2",
            start_date="20240922",
            end_date="20240922",
            start_hour="00",
            end_hour="23",
            metrics=["pcv_temperature_max", "pcv_temperature_avg"]
        )
        
        assert request.location_id == "A031"
        assert request.factory == "SinPyeong"
        assert request.building == "F-2001"
        assert request.area == "조립2"
        assert request.start_date == "20240922"
        assert request.end_date == "20240922"
        assert request.start_hour == "00"
        assert request.end_hour == "23"
        assert request.metrics == ["pcv_temperature_max", "pcv_temperature_avg"]

    def test_request_with_defaults(self):
        """Test request with default values"""
        request = AggregationRequest(
            start_date="20240922",
            end_date="20240922"
        )
        
        assert request.location_id is None
        assert request.factory is None
        assert request.building is None
        assert request.area is None
        assert request.start_hour == "00"
        assert request.end_hour == "23"
        assert request.metrics == ["pcv_temperature_max", "pcv_temperature_avg", "temperature_max", "temperature_avg", "humidity_max", "humidity_avg"]

    def test_request_validation(self):
        """Test request validation"""
        # Test missing required fields
        with pytest.raises(ValueError):
            AggregationRequest()  # Missing start_date and end_date


class TestHourlyData:
    """Test HourlyData model"""
    
    def test_valid_hourly_data(self):
        """Test valid hourly data"""
        hourly_data = HourlyData(
            ymd="20240922",
            hour="12",
            metrics={
                "pcv_temperature_max": "27.00",
                "pcv_temperature_avg": "27.00"
            }
        )
        
        assert hourly_data.ymd == "20240922"
        assert hourly_data.hour == "12"
        assert hourly_data.metrics["pcv_temperature_max"] == "27.00"
        assert hourly_data.metrics["pcv_temperature_avg"] == "27.00"

    def test_hourly_data_with_empty_metrics(self):
        """Test hourly data with empty metrics"""
        hourly_data = HourlyData(
            ymd="20240922",
            hour="12",
            metrics={}
        )
        
        assert hourly_data.ymd == "20240922"
        assert hourly_data.hour == "12"
        assert hourly_data.metrics == {}


class TestLocationData:
    """Test LocationData model"""
    
    def test_valid_location_data(self):
        """Test valid location data"""
        hourly_data = HourlyData(
            ymd="20240922",
            hour="12",
            metrics={"pcv_temperature_max": "27.00"}
        )
        
        location_data = LocationData(
            factory="SinPyeong",
            building="F-2001",
            floor=1,
            loc_id="A031",
            area="조립2",
            date=[hourly_data]
        )
        
        assert location_data.factory == "SinPyeong"
        assert location_data.building == "F-2001"
        assert location_data.floor == 1
        assert location_data.loc_id == "A031"
        assert location_data.area == "조립2"
        assert len(location_data.date) == 1
        assert location_data.date[0].ymd == "20240922"

    def test_location_data_with_empty_date(self):
        """Test location data with empty date list"""
        location_data = LocationData(
            factory="SinPyeong",
            building="F-2001",
            floor=1,
            loc_id="A031",
            area="조립2",
            date=[]
        )
        
        assert location_data.factory == "SinPyeong"
        assert len(location_data.date) == 0


class TestTemperatureAggregationResponse:
    """Test TemperatureAggregationResponse model"""
    
    def test_valid_response(self):
        """Test valid temperature aggregation response"""
        hourly_data = HourlyData(
            ymd="20240922",
            hour="12",
            metrics={"pcv_temperature_max": "27.00"}
        )
        
        location_data = LocationData(
            factory="SinPyeong",
            building="F-2001",
            floor=1,
            loc_id="A031",
            area="조립2",
            date=[hourly_data]
        )
        
        response = TemperatureAggregationResponse(locations=[location_data])
        
        assert response.locations[0].factory == "SinPyeong"
        assert response.locations[0].loc_id == "A031"
        assert len(response.locations[0].date) == 1

    def test_response_serialization(self):
        """Test response serialization to dict"""
        hourly_data = HourlyData(
            ymd="20240922",
            hour="12",
            metrics={"pcv_temperature_max": "27.00"}
        )
        
        location_data = LocationData(
            factory="SinPyeong",
            building="F-2001",
            floor=1,
            loc_id="A031",
            area="조립2",
            date=[hourly_data]
        )
        
        response = TemperatureAggregationResponse(locations=[location_data])
        response_dict = response.model_dump()
        
        assert "locations" in response_dict
        assert response_dict["locations"][0]["factory"] == "SinPyeong"
        assert response_dict["locations"][0]["loc_id"] == "A031"


# AggregationResponse 모델이 schemas.py에 없으므로 테스트 제거
# class TestAggregationResponse:
#     """Test AggregationResponse model"""
#     
#     def test_valid_aggregation_response(self):
#         """Test valid aggregation response"""
#         pass


class TestAggregationStats:
    """Test AggregationStats model"""
    
    def test_valid_stats(self):
        """Test valid aggregation stats"""
        stats = AggregationStats(
            total_count=100,
            avg_value=25.5,
            min_value=20.0,
            max_value=30.0,
            std_dev=2.5,
            percentiles={"25": 23.0, "50": 25.5, "75": 28.0}
        )
        
        assert stats.total_count == 100
        assert stats.avg_value == 25.5
        assert stats.min_value == 20.0
        assert stats.max_value == 30.0
        assert stats.std_dev == 2.5
        assert stats.percentiles["50"] == 25.5

    def test_stats_with_none_values(self):
        """Test stats with None values"""
        stats = AggregationStats(total_count=0)
        
        assert stats.total_count == 0
        assert stats.avg_value is None
        assert stats.min_value is None
        assert stats.max_value is None
        assert stats.std_dev is None
        assert stats.percentiles is None


class TestTimeSeriesData:
    """Test TimeSeriesData model"""
    
    def test_valid_time_series_data(self):
        """Test valid time series data"""
        ts_data = TimeSeriesData(
            timestamp=datetime(2024, 9, 22, 12, 0, 0),
            value=25.5,
            metadata={"sensor_id": "temp_001", "location": "A031"}
        )
        
        assert ts_data.timestamp == datetime(2024, 9, 22, 12, 0, 0)
        assert ts_data.value == 25.5
        assert ts_data.metadata["sensor_id"] == "temp_001"
        assert ts_data.metadata["location"] == "A031"

    def test_time_series_data_without_metadata(self):
        """Test time series data without metadata"""
        ts_data = TimeSeriesData(
            timestamp=datetime(2024, 9, 22, 12, 0, 0),
            value=25.5
        )
        
        assert ts_data.timestamp == datetime(2024, 9, 22, 12, 0, 0)
        assert ts_data.value == 25.5
        assert ts_data.metadata is None


class TestMetricValue:
    """Test MetricValue model"""
    
    def test_valid_metric_value(self):
        """Test valid metric value"""
        metric_value = MetricValue(
            value="27.00",
            status="normal"
        )
        
        assert metric_value.value == "27.00"
        assert metric_value.status == "normal"

    def test_metric_value_statuses(self):
        """Test different metric value statuses"""
        normal_metric = MetricValue(value="25.0", status="normal")
        warning_metric = MetricValue(value="45.0", status="warning")
        critical_metric = MetricValue(value="55.0", status="critical")
        
        assert normal_metric.status == "normal"
        assert warning_metric.status == "warning"
        assert critical_metric.status == "critical"
