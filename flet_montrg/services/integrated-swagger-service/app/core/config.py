"""
Configuration settings for API Dashboard Service
"""

import os
from typing import List, Union
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """애플리케이션 설정"""
    
    # 기본 설정
    app_name: str = "API Dashboard Service"
    version: str = "0.1.0"
    environment: str = Field(default="development", env="ENVIRONMENT")
    debug: bool = Field(default=False, env="DEBUG")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    # 서버 설정
    host: str = Field(default="0.0.0.0", env="HOST")
    port: int = Field(default=8000, env="PORT")
    
    # Kubernetes 설정
    kubernetes_namespace: str = Field(default="default", env="KUBERNETES_NAMESPACE")
    kubernetes_service_account_path: str = Field(
        default="/var/run/secrets/kubernetes.io/serviceaccount",
        env="KUBERNETES_SERVICE_ACCOUNT_PATH"
    )
    
    # 서비스 디스커버리 설정
    service_discovery_enabled: bool = Field(default=True, env="SERVICE_DISCOVERY_ENABLED")
    services_to_monitor: Union[List[str], str] = Field(
        default=[
            "aggregation-service",
            "location-service",
            "realtime-service",
            "thresholds-service",
            "alert-service",
            "alert-subscription-service",
            "alert-notification-service",
            "sensor-threshold-mapping-service"
        ],
        env="SERVICES_TO_MONITOR"
    )
    
    @field_validator('services_to_monitor')
    @classmethod
    def parse_services_to_monitor(cls, v):
        if isinstance(v, str):
            return [x.strip() for x in v.split(',') if x.strip()]
        return v
    
    # 대시보드 설정
    dashboard_title: str = Field(default="API Monitoring Dashboard", env="DASHBOARD_TITLE")
    dashboard_refresh_interval: int = Field(default=30, env="DASHBOARD_REFRESH_INTERVAL")
    
    # 모니터링 설정
    health_check_timeout: int = Field(default=10, env="HEALTH_CHECK_TIMEOUT")
    health_check_interval: int = Field(default=60, env="HEALTH_CHECK_INTERVAL")
    metrics_collection_interval: int = Field(default=30, env="METRICS_COLLECTION_INTERVAL")
    
    # 서비스 URL 설정 (다른 마이크로서비스와의 통신)
    aggregation_service_url: str = Field(default="http://aggregation-service:80", env="AGGREGATION_SERVICE_URL")
    location_service_url: str = Field(default="http://location-service:80", env="LOCATION_SERVICE_URL")
    realtime_service_url: str = Field(default="http://realtime-service:80", env="REALTIME_SERVICE_URL")
    thresholds_service_url: str = Field(default="http://thresholds-service:80", env="THRESHOLDS_SERVICE_URL")
    alert_service_url: str = Field(default="http://alert-service:80", env="ALERT_SERVICE_URL")
    alert_subscription_service_url: str = Field(default="http://alert-subscription-service:80", env="ALERT_SUBSCRIPTION_SERVICE_URL")
    alert_notification_service_url: str = Field(default="http://alert-notification-service:80", env="ALERT_NOTIFICATION_SERVICE_URL")
    sensor_threshold_mapping_service_url: str = Field(default="http://sensor-threshold-mapping-service:80", env="SENSOR_THRESHOLD_MAPPING_SERVICE_URL")
    
    # 데이터베이스 (선택사항)
    database_url: str = Field(default="", env="DATABASE_URL")
    
    # 인증 (선택사항)
    jwt_secret_key: str = Field(default="", env="JWT_SECRET_KEY")
    jwt_algorithm: str = Field(default="HS256", env="JWT_ALGORITHM")
    jwt_access_token_expire_minutes: int = Field(default=30, env="JWT_ACCESS_TOKEN_EXPIRE_MINUTES")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# 설정 인스턴스 생성
settings = Settings()


def get_settings() -> Settings:
    """설정 인스턴스 반환"""
    return settings