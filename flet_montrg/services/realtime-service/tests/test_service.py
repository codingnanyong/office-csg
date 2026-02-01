#!/usr/bin/env python3
"""
간단한 서비스 테스트 스크립트
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
from app.core.database import get_db
from app.services.temperature_service import TemperatureService
import pytest

@pytest.mark.asyncio
async def test_temperature_service():
    """온도 서비스 테스트"""
    try:
        print(f"Testing {settings.app_name}...")
        print(f"Database URL: {settings.database_url}")
        print(f"Location Service URL: {settings.location_service_url}")
        print(f"Thresholds Service URL: {settings.thresholds_service_url}")
        
        # 데이터베이스 연결 테스트
        async for db in get_db():
            service = TemperatureService(db)
            
            print("Getting current temperature data...")
            data = await service.get_current_temperature_data()
            
            print(f"Found {len(data)} temperature records")
            
            if data:
                print("Sample data:")
                sample = data[0]
                print(f"  - Sensor: {sample.location.area.loc_id if sample.location and sample.location.area else 'N/A'}")
                print(f"  - Time: {sample.capture_dt}")
                print(f"  - Temperature: {sample.location.area.temperature if sample.location and sample.location.area else 'N/A'}")
                print(f"  - PCV Temperature: {sample.location.area.pcv_temperature if sample.location and sample.location.area else 'N/A'}")
                print(f"  - Status: {sample.location.area.status if sample.location and sample.location.area else 'N/A'}")
            
            break
            
        print("Test completed successfully!")
        
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_temperature_service())
