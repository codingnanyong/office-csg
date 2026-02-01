#!/usr/bin/env python3
"""
Mock 데이터로 클라이언트 로직 테스트
"""
import asyncio
import sys
import os
from pathlib import Path
from decimal import Decimal

# 서비스 루트 디렉토리를 Python 경로에 추가
service_root = Path(__file__).parent.parent
if str(service_root) not in sys.path:
    sys.path.insert(0, str(service_root))

from app.core.logging import setup_logging, get_logger
from app.models.schemas import Location, Threshold, TemperatureCurrentData, LocationInfo, MeasurementData, MetricsData, MetricData
from app.services.temperature_service import TemperatureService
import pytest

# 로깅 설정
setup_logging()
logger = get_logger(__name__)

class MockLocationClient:
    """Mock Location Client"""
    
    async def get_location_by_sensor_id(self, sensor_id: str) -> Location:
        """Mock 위치 정보 반환"""
        return Location(
            loc_id=f"LOC_{sensor_id}",
            factory="테스트공장",
            building="테스트건물",
            floor=1,
            area="테스트구역",
            sensor_id=sensor_id
        )

class MockThresholdsClient:
    """Mock Thresholds Client"""
    
    async def get_applicable_thresholds(self, sensor_type: str) -> list[Threshold]:
        """Mock 임계치 정보 반환"""
        if sensor_type == "pcv_temperature":
            return [
                Threshold(
                    threshold_id=1,
                    threshold_type="pcv_temperature",
                    level="warning",
                    min_value=Decimal("20.0"),
                    max_value=Decimal("30.0"),
                    upd_dt=None
                ),
                Threshold(
                    threshold_id=2,
                    threshold_type="pcv_temperature",
                    level="critical",
                    min_value=Decimal("30.0"),
                    max_value=Decimal("40.0"),
                    upd_dt=None
                )
            ]
        return []

@pytest.mark.asyncio
async def test_threshold_logic():
    """임계치 검사 로직 테스트"""
    print("=" * 50)
    print("Testing Threshold Logic")
    print("=" * 50)
    
    # Mock 클라이언트 생성
    mock_location_client = MockLocationClient()
    mock_thresholds_client = MockThresholdsClient()
    
    # TemperatureService 인스턴스 생성 (DB 없이)
    service = TemperatureService(None)
    
    # 테스트 케이스들
    test_cases = [
        {"value": Decimal("25.0"), "expected_status": "warning"},
        {"value": Decimal("35.0"), "expected_status": "critical"},
        {"value": Decimal("15.0"), "expected_status": "normal"},
        {"value": Decimal("45.0"), "expected_status": "normal"},
    ]
    
    print("\nTesting threshold check logic:")
    for i, case in enumerate(test_cases, 1):
        value = case["value"]
        expected = case["expected_status"]
        
        # Mock 임계치 데이터
        thresholds = await mock_thresholds_client.get_applicable_thresholds("pcv_temperature")
        
        # 임계치 검사 실행
        status, is_alert, matched_threshold = service._check_thresholds(
            value, "pcv_temperature", thresholds
        )
        
        print(f"  {i}. Value: {value}°C")
        print(f"     Expected: {expected}, Got: {status}")
        print(f"     Alert: {is_alert}")
        if matched_threshold:
            print(f"     Matched threshold: {matched_threshold.level} ({matched_threshold.min_value}~{matched_threshold.max_value})")
        print()

@pytest.mark.asyncio
async def test_data_structure():
    """데이터 구조 테스트"""
    print("=" * 50)
    print("Testing Data Structure")
    print("=" * 50)
    
    # Mock 데이터 생성
    mock_location = Location(
        loc_id="LOC_001",
        factory="테스트공장",
        building="테스트건물",
        floor=2,
        area="테스트구역",
        sensor_id="SENSOR_001"
    )
    
    # LocationInfo 생성
    location_info = LocationInfo(
        loc_id="LOC_001",
        factory="테스트공장",
        building="테스트건물",
        floor=2,
        area="테스트구역"
    )
    
    # MetricsData 생성
    metrics_data = MetricsData(
        temperature=MetricData(value=Decimal("25.5"), status="normal"),
        humidity=MetricData(value=Decimal("60.2"), status="normal"),
        pcv_temperature=MetricData(value=Decimal("28.3"), status="warning")
    )
    
    # MeasurementData 생성
    measurement_data = MeasurementData(
        location=location_info,
        metrics=metrics_data
    )
    
    # TemperatureCurrentData 생성
    temp_data = TemperatureCurrentData(
        ymd="20240115",
        hh="10",
        measurements=[measurement_data]
    )
    
    print("Generated data structure:")
    if temp_data.measurements:
        measurement = temp_data.measurements[0]
        print(f"  Location ID: {measurement.location.loc_id}")
        print(f"  Time: {temp_data.ymd} {temp_data.hh}")
        if measurement.metrics.temperature:
            print(f"  Temperature: {measurement.metrics.temperature.value}°C")
        if measurement.metrics.humidity:
            print(f"  Humidity: {measurement.metrics.humidity.value}%")
        if measurement.metrics.pcv_temperature:
            print(f"  PCV Temperature: {measurement.metrics.pcv_temperature.value}°C")
            print(f"  Status: {measurement.metrics.pcv_temperature.status}")
        print(f"  Factory: {measurement.location.factory}")
        print(f"  Building: {measurement.location.building}")
        print(f"  Floor: {measurement.location.floor}")
    
    # JSON 변환 테스트
    print("\nJSON serialization test:")
    try:
        json_data = temp_data.model_dump()
        print("✅ JSON serialization successful")
        print(f"   Keys: {list(json_data.keys())}")
    except Exception as e:
        print(f"❌ JSON serialization failed: {e}")

async def main():
    """메인 테스트 함수"""
    print("Starting Mock Client Tests...")
    
    # 임계치 로직 테스트
    await test_threshold_logic()
    
    # 데이터 구조 테스트
    await test_data_structure()
    
    print("\n" + "=" * 50)
    print("All mock tests completed!")
    print("=" * 50)

if __name__ == "__main__":
    asyncio.run(main())
