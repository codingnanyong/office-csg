"""
Service models for API Dashboard
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum
from pydantic import BaseModel, Field


class ServiceStatus(str, Enum):
    """서비스 상태 열거형"""
    ONLINE = "online"
    OFFLINE = "offline"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


class HealthStatus(str, Enum):
    """헬스체크 상태 열거형"""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    WARNING = "warning"
    UNKNOWN = "unknown"


class ServiceEndpoint(BaseModel):
    """서비스 엔드포인트 정보"""
    name: str
    url: str
    method: str = "GET"
    path: str
    status_code: Optional[int] = None
    response_time: Optional[float] = None
    last_checked: Optional[datetime] = None
    error_message: Optional[str] = None


class ServiceMetrics(BaseModel):
    """서비스 메트릭 정보"""
    cpu_usage: Optional[float] = None
    memory_usage: Optional[float] = None
    request_count: int = 0
    error_count: int = 0
    avg_response_time: Optional[float] = None
    uptime: Optional[float] = None
    last_updated: datetime = Field(default_factory=datetime.now)


class KubernetesInfo(BaseModel):
    """Kubernetes 리소스 정보"""
    namespace: str
    deployment_name: Optional[str] = None
    service_name: Optional[str] = None
    replicas: int = 0
    ready_replicas: int = 0
    cluster_ip: Optional[str] = None
    ports: List[Dict[str, Any]] = []
    labels: Dict[str, str] = {}
    pods: List[Dict[str, Any]] = []


class MonitoredService(BaseModel):
    """모니터링 대상 서비스"""
    name: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    version: Optional[str] = None
    status: ServiceStatus = ServiceStatus.UNKNOWN
    health_status: HealthStatus = HealthStatus.UNKNOWN
    base_url: Optional[str] = None
    endpoints: List[ServiceEndpoint] = []
    metrics: Optional[ServiceMetrics] = None
    kubernetes_info: Optional[KubernetesInfo] = None
    tags: List[str] = []
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    last_health_check: Optional[datetime] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class ServiceOverview(BaseModel):
    """서비스 개요 정보"""
    total_services: int = 0
    online_services: int = 0
    offline_services: int = 0
    degraded_services: int = 0
    healthy_services: int = 0
    unhealthy_services: int = 0
    total_endpoints: int = 0
    total_pods: int = 0
    total_replicas: int = 0
    avg_response_time: Optional[float] = None
    total_requests: int = 0
    total_errors: int = 0
    last_updated: datetime = Field(default_factory=datetime.now)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }