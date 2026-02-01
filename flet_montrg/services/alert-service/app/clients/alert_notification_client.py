"""
Alert Notification service client
"""
import httpx
from typing import Optional
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class AlertNotificationClient:
    """Alert Notification service client for creating notifications"""
    
    def __init__(self):
        self.base_url = settings.alert_notification_service_url
        self.timeout = 30.0
    
    async def create_notification(
        self,
        alert_id: int,
        subscription_id: int,
        notify_type: str,
        notify_id: str
    ) -> Optional[dict]:
        """알림 생성"""
        try:
            data = {
                "alert_id": alert_id,
                "subscription_id": subscription_id,
                "notify_type": notify_type,
                "notify_id": notify_id
            }
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/api/v1/notifications/",
                    json=data
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error creating notification: {e}")
            return None
        except httpx.HTTPError as e:
            logger.error(f"Failed to create notification: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error creating notification: {e}")
            return None


# Global client instance
alert_notification_client = AlertNotificationClient()
