"""
Thresholds service client
"""
import httpx
from typing import List, Optional
from app.core.config import settings
from app.core.logging import get_logger
from app.models.schemas import Threshold

logger = get_logger(__name__)


class ThresholdsClient:
    """Thresholds service client for fetching threshold data"""
    
    def __init__(self):
        self.base_url = settings.thresholds_service_url
        self.timeout = 30.0
    
    async def get_all_thresholds(self, skip: int = 0, limit: int = 100) -> List[Threshold]:
        """모든 임계치 조회"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/thresholds/",
                    params={"skip": skip, "limit": limit}
                )
                response.raise_for_status()
                return [Threshold(**item) for item in response.json()]
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch all thresholds: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching all thresholds: {e}")
            return []
    
    async def get_threshold_by_id(self, threshold_id: int) -> Optional[Threshold]:
        """ID로 임계치 조회"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/thresholds/{threshold_id}"
                )
                response.raise_for_status()
                return Threshold(**response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning(f"Threshold not found for id: {threshold_id}")
                return None
            logger.error(f"HTTP error fetching threshold for id {threshold_id}: {e}")
            return None
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch threshold for id {threshold_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching threshold for id {threshold_id}: {e}")
            return None
    
    async def get_thresholds_by_type(self, threshold_type: str) -> List[Threshold]:
        """타입별 임계치 조회"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/thresholds/type/{threshold_type}"
                )
                response.raise_for_status()
                return [Threshold(**item) for item in response.json()]
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch thresholds for type {threshold_type}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching thresholds for type {threshold_type}: {e}")
            return []
    
    async def get_thresholds_by_type_and_level(self, threshold_type: str, level: str) -> List[Threshold]:
        """타입과 레벨로 임계치 조회"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/thresholds/type/{threshold_type}/level/{level}"
                )
                response.raise_for_status()
                return [Threshold(**item) for item in response.json()]
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch thresholds for type {threshold_type} and level {level}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching thresholds for type {threshold_type} and level {level}: {e}")
            return []
    
    async def get_applicable_thresholds(self, sensor_type: str) -> List[Threshold]:
        """센서 타입에 적용 가능한 임계치 조회"""
        try:
            # 센서 타입을 임계치 타입으로 매핑
            threshold_type_mapping = {
                "temperature": "temperature",
                "humidity": "humidity", 
                "pcv_temperature": "pcv_temperature"
            }
            
            threshold_type = threshold_type_mapping.get(sensor_type, "temperature")
            return await self.get_thresholds_by_type(threshold_type)
        except Exception as e:
            logger.error(f"Unexpected error getting applicable thresholds for sensor_type {sensor_type}: {e}")
            return []


# Global client instance
thresholds_client = ThresholdsClient()
