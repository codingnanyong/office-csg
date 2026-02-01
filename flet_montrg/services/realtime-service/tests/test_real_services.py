#!/usr/bin/env python3
"""
실제 location, thresholds 서비스 연결 테스트
"""
import asyncio
import httpx
import sys
import os
from pathlib import Path

# 서비스 루트 디렉토리를 Python 경로에 추가
service_root = Path(__file__).parent.parent
if str(service_root) not in sys.path:
    sys.path.insert(0, str(service_root))

from app.core.logging import setup_logging, get_logger
import pytest

# 로깅 설정
setup_logging()
logger = get_logger(__name__)

@pytest.mark.asyncio
async def test_location_service():
    """Location 서비스 직접 테스트"""
    print("=" * 50)
    print("Testing Location Service Direct Connection")
    print("=" * 50)
    
    location_url = "http://localhost:30002"
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            # 1. 헬스체크
            print(f"1. Health check: {location_url}/health")
            response = await client.get(f"{location_url}/health")
            print(f"   Status: {response.status_code}")
            if response.status_code == 200:
                print(f"   Response: {response.text}")
            else:
                print(f"   Error: {response.text}")
        except Exception as e:
            print(f"   ❌ Health check failed: {e}")
        
        try:
            # 2. 모든 위치 조회
            print(f"\n2. Get all locations: {location_url}/api/v1/locations/")
            response = await client.get(f"{location_url}/api/v1/locations/")
            print(f"   Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"   Found {len(data.get('data', []))} locations")
                if data.get('data'):
                    print("   Sample locations:")
                    for i, loc in enumerate(data['data'][:3]):
                        print(f"     {i+1}. Sensor ID: {loc.get('sensor_id')}, Location: {loc.get('loc_id')}")
                        print(f"        Factory: {loc.get('factory')}, Building: {loc.get('building')}")
            else:
                print(f"   Error: {response.text}")
        except Exception as e:
            print(f"   ❌ Get locations failed: {e}")
        
        try:
            # 3. 특정 센서 ID로 조회
            print(f"\n3. Get location by sensor ID: {location_url}/api/v1/locations/sensor/SENSOR001")
            response = await client.get(f"{location_url}/api/v1/locations/sensor/SENSOR001")
            print(f"   Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"   Location data: {data}")
            else:
                print(f"   Error: {response.text}")
        except Exception as e:
            print(f"   ❌ Get location by sensor failed: {e}")

@pytest.mark.asyncio
async def test_thresholds_service():
    """Thresholds 서비스 직접 테스트"""
    print("\n" + "=" * 50)
    print("Testing Thresholds Service Direct Connection")
    print("=" * 50)
    
    thresholds_url = "http://localhost:30001"
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            # 1. 헬스체크
            print(f"1. Health check: {thresholds_url}/health")
            response = await client.get(f"{thresholds_url}/health")
            print(f"   Status: {response.status_code}")
            if response.status_code == 200:
                print(f"   Response: {response.text}")
            else:
                print(f"   Error: {response.text}")
        except Exception as e:
            print(f"   ❌ Health check failed: {e}")
        
        try:
            # 2. 모든 임계치 조회
            print(f"\n2. Get all thresholds: {thresholds_url}/api/v1/thresholds/")
            response = await client.get(f"{thresholds_url}/api/v1/thresholds/")
            print(f"   Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"   Found {len(data.get('data', []))} thresholds")
                if data.get('data'):
                    print("   Sample thresholds:")
                    for i, thresh in enumerate(data['data'][:3]):
                        print(f"     {i+1}. Type: {thresh.get('threshold_type')}, Level: {thresh.get('level')}")
                        print(f"        Range: {thresh.get('min_value')} ~ {thresh.get('max_value')}")
            else:
                print(f"   Error: {response.text}")
        except Exception as e:
            print(f"   ❌ Get thresholds failed: {e}")
        
        try:
            # 3. pcv_temperature 임계치 조회
            print(f"\n3. Get pcv_temperature thresholds: {thresholds_url}/api/v1/thresholds/type/pcv_temperature")
            response = await client.get(f"{thresholds_url}/api/v1/thresholds/type/pcv_temperature")
            print(f"   Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"   Found {len(data.get('data', []))} pcv_temperature thresholds")
                if data.get('data'):
                    print("   PCV Temperature thresholds:")
                    for thresh in data['data']:
                        print(f"     - Level: {thresh.get('level')}, Range: {thresh.get('min_value')} ~ {thresh.get('max_value')}")
            else:
                print(f"   Error: {response.text}")
        except Exception as e:
            print(f"   ❌ Get pcv_temperature thresholds failed: {e}")

async def main():
    """메인 테스트 함수"""
    print("Starting Real Service Connection Tests...")
    print("Testing direct HTTP connections to NodePort services")
    print("=" * 50)
    
    # Location 서비스 테스트
    await test_location_service()
    
    # Thresholds 서비스 테스트
    await test_thresholds_service()
    
    print("\n" + "=" * 50)
    print("Real service connection tests completed!")
    print("=" * 50)

if __name__ == "__main__":
    asyncio.run(main())
