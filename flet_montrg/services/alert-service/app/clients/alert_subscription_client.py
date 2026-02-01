"""
Alert Subscription service client
"""
import httpx
from typing import List, Optional
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class AlertSubscriptionClient:
    """Alert Subscription service client for fetching subscription data"""
    
    def __init__(self):
        self.base_url = settings.alert_subscription_service_url
        self.timeout = 30.0
    
    async def get_matching_subscriptions(
        self,
        sensor_id: Optional[str] = None,
        plant: Optional[str] = None,
        factory: Optional[str] = None,
        building: Optional[str] = None,
        floor: Optional[int] = None,
        area: Optional[str] = None,
        threshold_type: Optional[str] = None,
        min_level: Optional[str] = None,
        enabled: bool = True
    ) -> List[dict]:
        """알람 조건에 맞는 구독 목록 조회"""
        try:
            params = {"enabled": enabled, "limit": 1000}
            if sensor_id:
                params["sensor_id"] = sensor_id
            if plant:
                params["plant"] = plant
            if factory:
                params["factory"] = factory
            if building:
                params["building"] = building
            if floor is not None:
                params["floor"] = floor
            if area:
                params["area"] = area
            if threshold_type:
                params["threshold_type"] = threshold_type
            if min_level:
                params["min_level"] = min_level
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/subscriptions/",
                    params=params
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching subscriptions: {e}")
            return []
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch subscriptions: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching subscriptions: {e}")
            return []


# Global client instance
alert_subscription_client = AlertSubscriptionClient()
