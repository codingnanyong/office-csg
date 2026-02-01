"""
Location service client
"""
import httpx
from typing import List, Optional
from app.core.config import settings
from app.core.logging import get_logger
from app.models.schemas import Location

logger = get_logger(__name__)


class LocationClient:
    """Location service client for fetching location data"""
    
    def __init__(self):
        self.base_url = settings.location_service_url
        self.timeout = 30.0
    
    async def get_all_locations(self, skip: int = 0, limit: int = 100) -> List[Location]:
        """모든 위치 정보 조회"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/location/",
                    params={"skip": skip, "limit": limit}
                )
                response.raise_for_status()
                return [Location(**item) for item in response.json()]
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch all locations: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching all locations: {e}")
            return []
    
    async def get_location_by_sensor_id(self, sensor_id: str) -> Optional[Location]:
        """센서 ID로 위치 정보 조회"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/location/{sensor_id}"
                )
                response.raise_for_status()
                return Location(**response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning(f"Location not found for sensor_id: {sensor_id}")
                return None
            logger.error(f"HTTP error fetching location for sensor_id {sensor_id}: {e}")
            return None
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch location for sensor_id {sensor_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching location for sensor_id {sensor_id}: {e}")
            return None
    
    async def get_locations_by_sensor_ids(self, sensor_ids: List[str]) -> List[Location]:
        """여러 센서 ID로 위치 정보 조회"""
        if not sensor_ids:
            return []
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/api/v1/locations/batch",
                    json={"sensor_ids": sensor_ids}
                )
                response.raise_for_status()
                return [Location(**item) for item in response.json()]
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch locations for sensor_ids {sensor_ids}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching locations for sensor_ids {sensor_ids}: {e}")
            return []
    
    async def get_location_by_loc_id(self, loc_id: str) -> Optional[Location]:
        """위치 ID로 위치 정보 조회"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/locations/loc/{loc_id}"
                )
                response.raise_for_status()
                return Location(**response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning(f"Location not found for loc_id: {loc_id}")
                return None
            logger.error(f"HTTP error fetching location for loc_id {loc_id}: {e}")
            return None
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch location for loc_id {loc_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching location for loc_id {loc_id}: {e}")
            return None


# Global client instance
location_client = LocationClient()
