#!/usr/bin/env python3
"""
Location과 Thresholds 클라이언트 테스트 스크립트
"""
import asyncio
import sys
import os
from pathlib import Path

# 서비스 루트 디렉토리를 Python 경로에 추가
service_root = Path(__file__).parent.parent
if str(service_root) not in sys.path:
    sys.path.insert(0, str(service_root))

from app.core.config import settings
from app.core.logging import setup_logging, get_logger
from app.clients.location_client import location_client
from app.clients.thresholds_client import thresholds_client

# 로깅 설정
setup_logging()
logger = get_logger(__name__)

import pytest

@pytest.mark.asyncio
async def test_location_client():
    """Location 클라이언트 테스트"""
    print("=" * 50)
    print("Testing Location Client")
    print("=" * 50)
    
    try:
        print(f"Location Service URL: {settings.location_service_url}")
        
        # 1. 모든 위치 정보 조회
        print("\n1. Getting all locations...")
        locations = await location_client.get_all_locations(limit=5)
        print(f"Found {len(locations)} locations")
        
        if locations:
            print("Sample locations:")
            for i, loc in enumerate(locations[:3]):
                print(f"  {i+1}. Sensor ID: {loc.sensor_id}, Location: {loc.loc_id}")
                print(f"     Factory: {loc.factory}, Building: {loc.building}")
        
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
                print("No location found")
        
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
    print("Testing Thresholds Client")
    print("=" * 50)
    
    try:
        print(f"Thresholds Service URL: {settings.thresholds_service_url}")
        
        # 1. 모든 임계치 조회
        print("\n1. Getting all thresholds...")
        thresholds = await thresholds_client.get_all_thresholds(limit=5)
        print(f"Found {len(thresholds)} thresholds")
        
        if thresholds:
            print("Sample thresholds:")
            for i, threshold in enumerate(thresholds[:3]):
                print(f"  {i+1}. ID: {threshold.threshold_id}")
                print(f"     Type: {threshold.threshold_type}")
                print(f"     Level: {threshold.level}")
                print(f"     Min: {threshold.min_value}, Max: {threshold.max_value}")
        
        # 2. 특정 타입의 임계치 조회
        print(f"\n2. Getting pcv_temperature thresholds...")
        pcv_thresholds = await thresholds_client.get_applicable_thresholds("pcv_temperature")
        print(f"Found {len(pcv_thresholds)} pcv_temperature thresholds")
        
        if pcv_thresholds:
            print("PCV Temperature thresholds:")
            for threshold in pcv_thresholds:
                print(f"  - Level: {threshold.level}")
                print(f"    Min: {threshold.min_value}, Max: {threshold.max_value}")
        
        # 3. 다른 타입의 임계치 조회
        print(f"\n3. Getting temperature thresholds...")
        temp_thresholds = await thresholds_client.get_applicable_thresholds("temperature")
        print(f"Found {len(temp_thresholds)} temperature thresholds")
        
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
    """통합 테스트 - 실제 센서 데이터와 함께"""
    print("\n" + "=" * 50)
    print("Integration Test")
    print("=" * 50)
    
    try:
        # 1. 센서 ID 목록 가져오기
        print("\n1. Getting sensor IDs from locations...")
        locations = await location_client.get_all_locations(limit=3)
        
        if not locations:
            print("No locations found, skipping integration test")
            return
        
        # 2. 각 센서에 대해 위치와 임계치 정보 조회
        for i, location in enumerate(locations[:2]):
            print(f"\n2.{i+1} Testing sensor: {location.sensor_id}")
            
            # 위치 정보
            print(f"   Location: {location.loc_id}")
            print(f"   Factory: {location.factory}, Building: {location.building}")
            
            # 임계치 정보
            thresholds = await thresholds_client.get_applicable_thresholds("pcv_temperature")
            print(f"   PCV Temperature thresholds: {len(thresholds)}")
            
            if thresholds:
                for threshold in thresholds:
                    print(f"     - {threshold.level}: {threshold.min_value} ~ {threshold.max_value}")
        
        print("✅ Integration test completed successfully!")
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()

async def main():
    """메인 테스트 함수"""
    print("Starting Client Tests...")
    print(f"Environment: {settings.environment}")
    print(f"Debug: {settings.debug}")
    
    # Set environment variables for actual service testing
    os.environ["LOCATION_SERVICE_URL"] = "http://localhost:30002" # NodePort for location-service
    os.environ["THRESHOLDS_SERVICE_URL"] = "http://localhost:30001" # NodePort for thresholds-service
    
    # Create new settings instance with updated env vars
    from app.core.config import Settings
    updated_settings = Settings()
    
    print(f"Location Service URL: {updated_settings.location_service_url}")
    print(f"Thresholds Service URL: {updated_settings.thresholds_service_url}")
    print("=" * 50)
    
    # Location 클라이언트 테스트
    await test_location_client()
    
    # Thresholds 클라이언트 테스트
    await test_thresholds_client()
    
    # 통합 테스트
    await test_integration()
    
    print("\n" + "=" * 50)
    print("All tests completed!")
    print("=" * 50)

if __name__ == "__main__":
    asyncio.run(main())
