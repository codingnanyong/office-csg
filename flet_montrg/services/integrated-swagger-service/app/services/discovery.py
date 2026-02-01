"""
Service discovery for Kubernetes services
"""

import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime

from ..core.config import settings
from ..core.kubernetes import get_kubernetes_client
from ..models.service import MonitoredService, ServiceStatus, HealthStatus, KubernetesInfo, ServiceEndpoint

logger = logging.getLogger(__name__)


class ServiceDiscovery:
    """Kubernetes 서비스 디스커버리"""
    
    def __init__(self):
        self.k8s_client = get_kubernetes_client()
        self.discovered_services: Dict[str, MonitoredService] = {}
        
        # 기본 서비스 엔드포인트 정의
        self.service_endpoints = {
            "aggregation-service": [
                {"name": "health", "path": "/health", "method": "GET"},
                {"name": "docs", "path": "/docs", "method": "GET"},
                {"name": "aggregations", "path": "/api/v1/aggregations", "method": "GET"},
            ],
            "alert-service": [
                {"name": "health", "path": "/health", "method": "GET"},
                {"name": "docs", "path": "/docs", "method": "GET"},
                {"name": "alerts", "path": "/api/v1/alerts", "method": "GET"},
            ],
            "alert-subscription-service": [
                {"name": "health", "path": "/health", "method": "GET"},
                {"name": "docs", "path": "/docs", "method": "GET"},
                {"name": "subscriptions", "path": "/api/v1/subscriptions", "method": "GET"},
            ],
            "alert-notification-service": [
                {"name": "health", "path": "/health", "method": "GET"},
                {"name": "docs", "path": "/docs", "method": "GET"},
                {"name": "notifications", "path": "/api/v1/notifications", "method": "GET"},
            ],
            "alert-history-service": [
                {"name": "health", "path": "/health", "method": "GET"},
                {"name": "docs", "path": "/docs", "method": "GET"},
                {"name": "history", "path": "/api/v1/alert-history", "method": "GET"},
            ],
            "location-service": [
                {"name": "health", "path": "/health", "method": "GET"},
                {"name": "docs", "path": "/docs", "method": "GET"},
                {"name": "locations", "path": "/api/v1/locations", "method": "GET"},
            ],
            "realtime-service": [
                {"name": "health", "path": "/health", "method": "GET"},
                {"name": "docs", "path": "/docs", "method": "GET"},
                {"name": "realtime", "path": "/api/v1/realtime", "method": "GET"},
            ],
            "thresholds-service": [
                {"name": "health", "path": "/health", "method": "GET"},
                {"name": "docs", "path": "/docs", "method": "GET"},
                {"name": "thresholds", "path": "/api/v1/thresholds", "method": "GET"},
            ],
            "sensor-threshold-mapping-service": [
                {"name": "health", "path": "/health", "method": "GET"},
                {"name": "docs", "path": "/docs", "method": "GET"},
                {"name": "mappings", "path": "/api/v1/mappings", "method": "GET"},
            ]
        }
    
    async def discover_services(self) -> Dict[str, MonitoredService]:
        """k8s에서 서비스들을 검색하여 모니터링 서비스 목록 생성"""
        if not self.k8s_client or not self.k8s_client.is_connected():
            logger.warning("Kubernetes client not available, using fallback discovery")
            return await self._fallback_discovery()
        
        try:
            # k8s 서비스 조회
            k8s_services = await self.k8s_client.get_services(settings.kubernetes_namespace)
            k8s_deployments = await self.k8s_client.get_deployments(settings.kubernetes_namespace)
            
            # 서비스별로 정보 수집
            for service_name in settings.services_to_monitor:
                try:
                    # k8s 서비스 정보 찾기
                    k8s_service = next((s for s in k8s_services if s["name"] == service_name), None)
                    k8s_deployment = next((d for d in k8s_deployments if d["name"] == service_name), None)
                    
                    if k8s_service:
                        monitored_service = await self._create_monitored_service(
                            service_name, k8s_service, k8s_deployment
                        )
                        self.discovered_services[service_name] = monitored_service
                        logger.info(f"Discovered service: {service_name}")
                    else:
                        logger.warning(f"Service not found in k8s: {service_name}")
                        # 기본 서비스 정보로 생성
                        self.discovered_services[service_name] = self._create_fallback_service(service_name)
                
                except Exception as e:
                    logger.error(f"Error discovering service {service_name}: {e}")
                    self.discovered_services[service_name] = self._create_fallback_service(service_name)
            
            logger.info(f"Service discovery completed: {len(self.discovered_services)} services found")
            return self.discovered_services
            
        except Exception as e:
            logger.error(f"Service discovery failed: {e}")
            return await self._fallback_discovery()
    
    async def _create_monitored_service(
        self, 
        service_name: str, 
        k8s_service: Dict, 
        k8s_deployment: Optional[Dict] = None
    ) -> MonitoredService:
        """k8s 정보를 기반으로 MonitoredService 생성"""
        
        # 기본 URL 구성
        cluster_ip = k8s_service.get("cluster_ip", "")
        ports = k8s_service.get("ports", [])
        http_port = next((p["port"] for p in ports if p.get("name") in ["http", "web"] or p["port"] in [8000, 80]), None)
        if http_port is None and ports:
            http_port = ports[0]["port"]
        
        base_url = f"http://{cluster_ip}:{http_port}" if cluster_ip and http_port else None
        
        # 엔드포인트 생성
        endpoints = []
        if base_url and service_name in self.service_endpoints:
            for ep_config in self.service_endpoints[service_name]:
                endpoint = ServiceEndpoint(
                    name=ep_config["name"],
                    url=f"{base_url}{ep_config['path']}",
                    method=ep_config["method"],
                    path=ep_config["path"]
                )
                endpoints.append(endpoint)
        
        # Kubernetes 정보
        k8s_info = KubernetesInfo(
            namespace=k8s_service["namespace"],
            deployment_name=k8s_deployment["name"] if k8s_deployment else None,
            service_name=k8s_service["name"],
            replicas=k8s_deployment["replicas"] if k8s_deployment else 0,
            ready_replicas=k8s_deployment["ready_replicas"] if k8s_deployment else 0,
            cluster_ip=cluster_ip,
            ports=ports,
            labels=k8s_service.get("selector", {}),
            pods=[]
        )
        
        # 파드 정보 추가
        if k8s_deployment and k8s_info.labels:
            label_selector = ",".join([f"{k}={v}" for k, v in k8s_info.labels.items()])
            pods = await self.k8s_client.get_pods(
                namespace=settings.kubernetes_namespace,
                label_selector=label_selector
            )
            k8s_info.pods = pods
        
        return MonitoredService(
            name=service_name,
            display_name=service_name.replace("-", " ").title(),
            description=f"{service_name} microservice",
            status=ServiceStatus.UNKNOWN,
            health_status=HealthStatus.UNKNOWN,
            base_url=base_url,
            endpoints=endpoints,
            kubernetes_info=k8s_info,
            tags=["microservice", "api"],
            updated_at=datetime.now()
        )
    
    def _create_fallback_service(self, service_name: str) -> MonitoredService:
        """k8s 정보가 없을 때 기본 서비스 정보 생성"""
        
        # ConfigMap에서 설정된 서비스 URL 사용
        service_urls = {
            "aggregation-service": settings.aggregation_service_url,
            "location-service": settings.location_service_url,
            "realtime-service": settings.realtime_service_url,
            "thresholds-service": settings.thresholds_service_url,
            "alert-service": settings.alert_service_url,
            "alert-subscription-service": settings.alert_subscription_service_url,
            "alert-notification-service": settings.alert_notification_service_url,
            "sensor-threshold-mapping-service": settings.sensor_threshold_mapping_service_url
        }
        
        base_url = service_urls.get(service_name, f"http://{service_name}:80")
        
        # 엔드포인트 생성
        endpoints = []
        if service_name in self.service_endpoints:
            for ep_config in self.service_endpoints[service_name]:
                endpoint = ServiceEndpoint(
                    name=ep_config["name"],
                    url=f"{base_url}{ep_config['path']}",
                    method=ep_config["method"],
                    path=ep_config["path"]
                )
                endpoints.append(endpoint)
        
        return MonitoredService(
            name=service_name,
            display_name=service_name.replace("-", " ").title(),
            description=f"{service_name} microservice",
            status=ServiceStatus.UNKNOWN,
            health_status=HealthStatus.UNKNOWN,
            base_url=base_url,
            endpoints=endpoints,
            tags=["microservice", "api"],
            updated_at=datetime.now()
        )
    
    async def _fallback_discovery(self) -> Dict[str, MonitoredService]:
        """k8s 클라이언트를 사용할 수 없을 때 fallback 서비스 디스커버리"""
        logger.info("Using fallback service discovery")
        
        fallback_services = {}
        for service_name in settings.services_to_monitor:
            fallback_services[service_name] = self._create_fallback_service(service_name)
        
        return fallback_services
    
    async def refresh_services(self) -> Dict[str, MonitoredService]:
        """서비스 목록 새로고침"""
        return await self.discover_services()
    
    def get_service(self, service_name: str) -> Optional[MonitoredService]:
        """특정 서비스 정보 조회"""
        return self.discovered_services.get(service_name)
    
    def get_all_services(self) -> Dict[str, MonitoredService]:
        """모든 서비스 정보 조회"""
        return self.discovered_services.copy()
    
    async def update_service_endpoints(self, service_name: str) -> bool:
        """서비스 엔드포인트 정보 업데이트"""
        if service_name not in self.discovered_services:
            return False
        
        try:
            if self.k8s_client and self.k8s_client.is_connected():
                endpoint_urls = await self.k8s_client.get_service_endpoints(
                    service_name, 
                    settings.kubernetes_namespace
                )
                
                service = self.discovered_services[service_name]
                if endpoint_urls and service_name in self.service_endpoints:
                    # 새 엔드포인트 목록 생성
                    new_endpoints = []
                    base_url = endpoint_urls[0].rsplit(':', 1)[0] + f":{endpoint_urls[0].split(':')[-1]}"
                    
                    for ep_config in self.service_endpoints[service_name]:
                        endpoint = ServiceEndpoint(
                            name=ep_config["name"],
                            url=f"{base_url}{ep_config['path']}",
                            method=ep_config["method"],
                            path=ep_config["path"]
                        )
                        new_endpoints.append(endpoint)
                    
                    service.endpoints = new_endpoints
                    service.base_url = base_url
                    service.updated_at = datetime.now()
                
                return True
                
        except Exception as e:
            logger.error(f"Failed to update service endpoints for {service_name}: {e}")
            return False


# 전역 서비스 디스커버리 인스턴스
_service_discovery: Optional[ServiceDiscovery] = None


def get_service_discovery() -> ServiceDiscovery:
    """서비스 디스커버리 인스턴스 반환"""
    global _service_discovery
    
    if _service_discovery is None:
        _service_discovery = ServiceDiscovery()
    
    return _service_discovery