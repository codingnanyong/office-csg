"""
Thresholds service client
"""
import httpx
from typing import Optional
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class ThresholdsClient:
    """Thresholds service client for fetching threshold data"""
    
    def __init__(self):
        self.base_url = settings.thresholds_service_url
        self.timeout = 30.0
    
    async def get_threshold(self, threshold_id: int) -> Optional[dict]:
        """임계치 조회"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/thresholds/{threshold_id}"
                )
                response.raise_for_status()
                return response.json()
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


# Global client instance
thresholds_client = ThresholdsClient()
