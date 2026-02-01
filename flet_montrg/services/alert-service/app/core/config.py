"""
Application configuration management
"""
import os
from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import field_validator


class Settings(BaseSettings):
    """Application settings using Pydantic BaseSettings"""
    
    # Application settings
    app_name: str = "Alert Service"
    app_version: str = "1.0.0"
    debug: bool = False
    
    # Server settings
    host: str = "0.0.0.0"
    port: int = 8000
    
    # Database settings
    database_url: str = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/hq")
    
    # CORS settings
    cors_origins: List[str] = ["*"]
    
    # Logging settings
    log_level: str = "INFO"
    
    # Environment
    environment: str = "development"
    
    # External service URLs
    thresholds_service_url: str = os.getenv("THRESHOLDS_SERVICE_URL", "http://thresholds-service:80")
    location_service_url: str = os.getenv("LOCATION_SERVICE_URL", "http://location-service:80")
    alert_subscription_service_url: str = os.getenv("ALERT_SUBSCRIPTION_SERVICE_URL", "http://alert-subscription-service:80")
    alert_notification_service_url: str = os.getenv("ALERT_NOTIFICATION_SERVICE_URL", "http://alert-notification-service:80")
    # sensor_threshold_mapping_service_url: 미구현 서비스 - 구현 시 추가 예정
    
    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors_origins(cls, v):
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v
    
    model_config = {
        "env_file": ".env",
        "case_sensitive": False
    }


# Global settings instance
settings = Settings()
