#!/usr/bin/env python3
"""
Kubernetes 환경에서 실제 서비스 연결 테스트
"""
import asyncio
import sys
import os
from pathlib import Path

# 서비스 루트 디렉토리를 Python 경로에 추가
service_root = Path(__file__).parent.parent
if str(service_root) not in sys.path:
    sys.path.insert(0, str(service_root))

from app.core.logging import setup_logging, get_logger
from app.clients.location_client import LocationClient
from app.clients.thresholds_client import ThresholdsClient
import pytest

# 로깅 설정
setup_logging()
logger = get_logger(__name__)

@pytest.mark.asyncio
async def test_kubernetes_services():
    """Kubernetes 환경에서 서비스 연결 테스트"""
    print("=" * 60)
    print("Testing Kubernetes Service Connections")
    print("=" * 60)
    
    # Kubernetes 서비스명으로 클라이언트 생성
    location_client = LocationClient()
    thresholds_client = ThresholdsClient()
    
    print(f"Location Service URL: {location_client.base_url}")
    print(f"Thresholds Service URL: {thresholds_client.base_url}")
    print()
    
    # Location Service 테스트
    print("1. Testing Location Service...")
    try:
        locations = await location_client.get_all_locations(limit=3)
        print(f"   ✅ Success: Found {len(locations)} locations")
        if locations:
            print(f"   Sample: {locations[0].sensor_id} → {locations[0].loc_id}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
    
    # Thresholds Service 테스트
    print("\n2. Testing Thresholds Service...")
    try:
        thresholds = await thresholds_client.get_all_thresholds(limit=3)
        print(f"   ✅ Success: Found {len(thresholds)} thresholds")
        if thresholds:
            print(f"   Sample: {thresholds[0].threshold_type} - {thresholds[0].level}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
    
    # PCV Temperature 임계치 테스트
    print("\n3. Testing PCV Temperature Thresholds...")
    try:
        pcv_thresholds = await thresholds_client.get_applicable_thresholds("pcv_temperature")
        print(f"   ✅ Success: Found {len(pcv_thresholds)} pcv_temperature thresholds")
        if pcv_thresholds:
            for thresh in pcv_thresholds:
                print(f"   - {thresh.level}: {thresh.min_value} ~ {thresh.max_value}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
    
    print("\n" + "=" * 60)
    print("Kubernetes service connection test completed!")
    print("=" * 60)

async def main():
    """메인 테스트 함수"""
    print("Starting Kubernetes Service Connection Test...")
    print("This test uses the actual Kubernetes service names:")
    print("- location-service:80")
    print("- thresholds-service:80")
    print()
    print("Note: This will only work when running inside Kubernetes cluster")
    print("or when the services are accessible via port-forwarding.")
    print()
    
    await test_kubernetes_services()

if __name__ == "__main__":
    asyncio.run(main())
