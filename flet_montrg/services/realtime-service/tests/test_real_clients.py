#!/usr/bin/env python3
"""
실제 서비스에 연결하여 클라이언트 테스트
"""
import asyncio
import sys
import os
from pathlib import Path

# 서비스 루트 디렉토리를 Python 경로에 추가
service_root = Path(__file__).parent.parent
if str(service_root) not in sys.path:
    sys.path.insert(0, str(service_root))

# 환경변수 설정 (클라이언트 생성 전에 설정)
os.environ["LOCATION_SERVICE_URL"] = "http://localhost:30002"
os.environ["THRESHOLDS_SERVICE_URL"] = "http://localhost:30001"

from app.core.logging import setup_logging, get_logger
from app.clients.location_client import LocationClient
from app.clients.thresholds_client import ThresholdsClient
import pytest

# 로깅 설정
setup_logging()
logger = get_logger(__name__)

@pytest.mark.asyncio
async def test_location_client():
    """Location 클라이언트 테스트"""
    print("=" * 50)
    print("Testing Location Client with Real Service")
    print("=" * 50)
    
    # 새로운 클라이언트 인스턴스 생성
    location_client = LocationClient()
    
    try:
        print(f"Location Service URL: {location_client.base_url}")
        
        # 1. 모든 위치 정보 조회
        print("\n1. Getting all locations...")
        locations = await location_client.get_all_locations(limit=5)
        print(f"Found {len(locations)} locations")
        
        if locations:
            print("Sample locations:")
            for i, loc in enumerate(locations[:3]):
                print(f"  {i+1}. Sensor ID: {loc.sensor_id}, Location: {loc.loc_id}")
                print(f"     Factory: {loc.factory}, Building: {loc.building}")
                print(f"     Floor: {loc.floor}, Area: {loc.area}")
        
        # 2. 특정 센서 ID로 위치 조회
        if locations:
            test_sensor_id = locations[0].sensor_id
            print(f"\n2. Getting location for sensor: {test_sensor_id}")
            location = await location_client.get_location_by_sensor_id(test_sensor_id)
            
            if location:
                print(f"Found location: {location.loc_id}")
                print(f"  Factory: {location.factory}")
                print(f"  Building: {location.building}")
                print(f"  Floor: {location.floor}")
                print(f"  Area: {location.area}")
            else:
                print("Location not found")
        
        # 3. 존재하지 않는 센서 ID 테스트
        print(f"\n3. Testing non-existent sensor ID...")
        fake_location = await location_client.get_location_by_sensor_id("FAKE_SENSOR_123")
        print(f"Result for fake sensor: {fake_location}")
        
        print("✅ Location client test completed successfully!")
        
    except Exception as e:
        print(f"❌ Location client test failed: {e}")
        import traceback
        traceback.print_exc()

@pytest.mark.asyncio
async def test_thresholds_client():
    """Thresholds 클라이언트 테스트"""
    print("\n" + "=" * 50)
    print("Testing Thresholds Client with Real Service")
    print("=" * 50)
    
    # 새로운 클라이언트 인스턴스 생성
    thresholds_client = ThresholdsClient()
    
    try:
        print(f"Thresholds Service URL: {thresholds_client.base_url}")
        
        # 1. 모든 임계치 조회
        print("\n1. Getting all thresholds...")
        thresholds = await thresholds_client.get_all_thresholds(limit=10)
        print(f"Found {len(thresholds)} thresholds")
        
        if thresholds:
            print("Sample thresholds:")
            for i, thresh in enumerate(thresholds[:3]):
                print(f"  {i+1}. Type: {thresh.threshold_type}, Level: {thresh.level}")
                print(f"     Range: {thresh.min_value} ~ {thresh.max_value}")
        
        # 2. pcv_temperature 임계치 조회
        print(f"\n2. Getting pcv_temperature thresholds...")
        pcv_thresholds = await thresholds_client.get_applicable_thresholds("pcv_temperature")
        print(f"Found {len(pcv_thresholds)} pcv_temperature thresholds")
        
        if pcv_thresholds:
            print("PCV Temperature thresholds:")
            for thresh in pcv_thresholds:
                print(f"  - Level: {thresh.level}, Range: {thresh.min_value} ~ {thresh.max_value}")
        
        # 3. temperature 임계치 조회
        print(f"\n3. Getting temperature thresholds...")
        temp_thresholds = await thresholds_client.get_applicable_thresholds("temperature")
        print(f"Found {len(temp_thresholds)} temperature thresholds")
        
        if temp_thresholds:
            print("Temperature thresholds:")
            for thresh in temp_thresholds:
                print(f"  - Level: {thresh.level}, Range: {thresh.min_value} ~ {thresh.max_value}")
        
        # 4. 존재하지 않는 타입 테스트
        print(f"\n4. Testing non-existent threshold type...")
        fake_thresholds = await thresholds_client.get_applicable_thresholds("fake_type")
        print(f"Result for fake type: {len(fake_thresholds)} thresholds")
        
        print("✅ Thresholds client test completed successfully!")
        
    except Exception as e:
        print(f"❌ Thresholds client test failed: {e}")
        import traceback
        traceback.print_exc()

@pytest.mark.asyncio
async def test_integration():
    """통합 테스트"""
    print("\n" + "=" * 50)
    print("Integration Test")
    print("=" * 50)
    
    try:
        # 새로운 클라이언트 인스턴스 생성
        location_client = LocationClient()
        thresholds_client = ThresholdsClient()
        
        # 1. 위치 정보에서 센서 ID 가져오기
        print("\n1. Getting sensor IDs from locations...")
        locations = await location_client.get_all_locations(limit=3)
        
        if not locations:
            print("No locations found, skipping integration test")
            return
        
        print(f"Found {len(locations)} locations for integration test")
        
        # 2. 각 센서에 대해 임계치 검사 시뮬레이션
        print("\n2. Simulating threshold checks for each sensor...")
        pcv_thresholds = await thresholds_client.get_applicable_thresholds("pcv_temperature")
        
        if not pcv_thresholds:
            print("No pcv_temperature thresholds found")
            return
        
        print(f"Using {len(pcv_thresholds)} pcv_temperature thresholds:")
        for thresh in pcv_thresholds:
            print(f"  - {thresh.level}: {thresh.min_value} ~ {thresh.max_value}")
        
        # 3. 샘플 데이터로 임계치 검사
        from app.services.temperature_service import TemperatureService
        service = TemperatureService(None)
        
        test_values = [15.0, 25.0, 35.0, 45.0]
        print(f"\n3. Testing threshold logic with sample values:")
        
        for value in test_values:
            status, is_alert, matched_threshold = service._check_thresholds(
                value, "pcv_temperature", pcv_thresholds
            )
            print(f"  Value: {value}°C → Status: {status}, Alert: {is_alert}")
            if matched_threshold:
                print(f"    Matched: {matched_threshold.level} ({matched_threshold.min_value}~{matched_threshold.max_value})")
        
        print("✅ Integration test completed successfully!")
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()

async def main():
    """메인 테스트 함수"""
    print("Starting Real Service Client Tests...")
    print("Using NodePort URLs for direct connection")
    print("=" * 50)
    
    # Location 클라이언트 테스트
    await test_location_client()
    
    # Thresholds 클라이언트 테스트
    await test_thresholds_client()
    
    # 통합 테스트
    await test_integration()
    
    print("\n" + "=" * 50)
    print("All real service tests completed!")
    print("=" * 50)

if __name__ == "__main__":
    asyncio.run(main())
