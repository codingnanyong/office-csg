"""
Service monitoring and health checking
"""

import asyncio
import httpx
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import time

from ..core.config import settings
from ..models.service import (
    MonitoredService, 
    ServiceStatus, 
    HealthStatus, 
    ServiceMetrics,
    ServiceEndpoint
)
from .discovery import get_service_discovery

logger = logging.getLogger(__name__)


class ServiceMonitor:
    """서비스 모니터링 클래스"""
    
    def __init__(self):
        self.service_discovery = get_service_discovery()
        self.monitoring_active = False
        self.monitoring_task = None
        
        # HTTP 클라이언트 설정
        self.http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.health_check_timeout),
            follow_redirects=True
        )
        
        # 모니터링 데이터 저장
        self.service_metrics_history: Dict[str, List[ServiceMetrics]] = {}
        self.last_check_times: Dict[str, datetime] = {}
    
    async def start_monitoring(self):
        """모니터링 시작"""
        if self.monitoring_active:
            logger.warning("Monitoring is already active")
            return
        
        logger.info("Starting service monitoring")
        self.monitoring_active = True
        
        # 초기 서비스 디스커버리
        await self.service_discovery.discover_services()
        
        # 백그라운드 모니터링 태스크 시작
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
    
    async def stop_monitoring(self):
        """모니터링 중지"""
        logger.info("Stopping service monitoring")
        self.monitoring_active = False
        
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        
        await self.http_client.aclose()
    
    async def _monitoring_loop(self):
        """백그라운드 모니터링 루프"""
        while self.monitoring_active:
            try:
                # 모든 서비스 상태 체크
                await self.check_all_services()
                
                # 설정된 간격만큼 대기
                await asyncio.sleep(settings.health_check_interval)
                
            except asyncio.CancelledError:
                logger.info("Monitoring loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(10)  # 에러 발생 시 10초 대기
    
    async def check_all_services(self) -> Dict[str, MonitoredService]:
        """모든 서비스 상태 체크"""
        services = self.service_discovery.get_all_services()
        
        if not services:
            logger.warning("No services found for monitoring")
            return {}
        
        # 동시에 모든 서비스 체크
        tasks = []
        for service_name, service in services.items():
            task = asyncio.create_task(self.check_service(service))
            tasks.append((service_name, task))
        
        # 결과 수집
        updated_services = {}
        for service_name, task in tasks:
            try:
                updated_service = await task
                updated_services[service_name] = updated_service
                self.last_check_times[service_name] = datetime.now()
            except Exception as e:
                logger.error(f"Error checking service {service_name}: {e}")
                # 에러 발생 시에도 기존 서비스 정보는 유지
                service = services[service_name]
                service.status = ServiceStatus.OFFLINE
                service.health_status = HealthStatus.UNHEALTHY
                service.updated_at = datetime.now()
                updated_services[service_name] = service
        
        # 디스커버리 서비스의 데이터 업데이트
        self.service_discovery.discovered_services.update(updated_services)
        
        return updated_services
    
    async def check_service(self, service: MonitoredService) -> MonitoredService:
        """단일 서비스 상태 체크"""
        logger.debug(f"Checking service: {service.name}")
        
        # 엔드포인트 체크
        total_endpoints = len(service.endpoints)
        healthy_endpoints = 0
        total_response_time = 0
        
        for endpoint in service.endpoints:
            endpoint_healthy, response_time = await self._check_endpoint(endpoint)
            if endpoint_healthy:
                healthy_endpoints += 1
            if response_time:
                total_response_time += response_time
        
        # 서비스 상태 결정
        if total_endpoints == 0:
            service.status = ServiceStatus.UNKNOWN
            service.health_status = HealthStatus.UNKNOWN
        elif healthy_endpoints == total_endpoints:
            service.status = ServiceStatus.ONLINE
            service.health_status = HealthStatus.HEALTHY
        elif healthy_endpoints > 0:
            service.status = ServiceStatus.DEGRADED
            service.health_status = HealthStatus.WARNING
        else:
            service.status = ServiceStatus.OFFLINE
            service.health_status = HealthStatus.UNHEALTHY
        
        # 메트릭 업데이트
        avg_response_time = total_response_time / max(healthy_endpoints, 1) if healthy_endpoints > 0 else None
        
        metrics = ServiceMetrics(
            avg_response_time=avg_response_time,
            last_updated=datetime.now()
        )
        
        service.metrics = metrics
        service.last_health_check = datetime.now()
        service.updated_at = datetime.now()
        
        # 메트릭 히스토리 저장
        self._store_metrics_history(service.name, metrics)
        
        logger.debug(f"Service {service.name} check completed: {service.status}")
        return service
    
    async def _check_endpoint(self, endpoint: ServiceEndpoint) -> Tuple[bool, Optional[float]]:
        """개별 엔드포인트 상태 체크"""
        start_time = time.time()
        
        try:
            response = await self.http_client.request(
                method=endpoint.method,
                url=endpoint.url,
            )
            
            response_time = (time.time() - start_time) * 1000  # ms 단위
            
            # 엔드포인트 정보 업데이트
            endpoint.status_code = response.status_code
            endpoint.response_time = response_time
            endpoint.last_checked = datetime.now()
            endpoint.error_message = None
            
            # 2xx, 3xx 상태 코드를 정상으로 간주
            is_healthy = 200 <= response.status_code < 400
            
            return is_healthy, response_time
            
        except httpx.TimeoutException:
            response_time = (time.time() - start_time) * 1000
            endpoint.status_code = None
            endpoint.response_time = response_time
            endpoint.last_checked = datetime.now()
            endpoint.error_message = "Request timeout"
            return False, response_time
            
        except httpx.ConnectError:
            response_time = (time.time() - start_time) * 1000
            endpoint.status_code = None
            endpoint.response_time = response_time
            endpoint.last_checked = datetime.now()
            endpoint.error_message = "Connection failed"
            return False, response_time
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            endpoint.status_code = None
            endpoint.response_time = response_time
            endpoint.last_checked = datetime.now()
            endpoint.error_message = str(e)
            return False, response_time
    
    def _store_metrics_history(self, service_name: str, metrics: ServiceMetrics):
        """메트릭 히스토리 저장 (메모리 기반)"""
        if service_name not in self.service_metrics_history:
            self.service_metrics_history[service_name] = []
        
        # 최대 100개 히스토리 유지
        history = self.service_metrics_history[service_name]
        history.append(metrics)
        
        if len(history) > 100:
            history.pop(0)
    
    def get_service_metrics_history(self, service_name: str, limit: int = 50) -> List[ServiceMetrics]:
        """서비스 메트릭 히스토리 조회"""
        history = self.service_metrics_history.get(service_name, [])
        return history[-limit:] if limit else history
    
    async def check_service_by_name(self, service_name: str) -> Optional[MonitoredService]:
        """특정 서비스 즉시 체크"""
        service = self.service_discovery.get_service(service_name)
        if not service:
            logger.warning(f"Service not found: {service_name}")
            return None
        
        return await self.check_service(service)
    
    async def refresh_and_check_all(self) -> Dict[str, MonitoredService]:
        """서비스 디스커버리 새로고침 후 모든 서비스 체크"""
        logger.info("Refreshing services and checking all")
        
        # 서비스 디스커버리 새로고침
        await self.service_discovery.refresh_services()
        
        # 모든 서비스 체크
        return await self.check_all_services()
    
    def get_monitoring_status(self) -> Dict:
        """모니터링 상태 정보 반환"""
        return {
            "monitoring_active": self.monitoring_active,
            "last_check_times": {
                name: time.isoformat() for name, time in self.last_check_times.items()
            },
            "monitored_services_count": len(self.service_discovery.get_all_services()),
            "check_interval": settings.health_check_interval,
            "timeout": settings.health_check_timeout
        }


# 전역 서비스 모니터 인스턴스
_service_monitor: Optional[ServiceMonitor] = None


def get_service_monitor() -> ServiceMonitor:
    """서비스 모니터 인스턴스 반환"""
    global _service_monitor
    
    if _service_monitor is None:
        _service_monitor = ServiceMonitor()
    
    return _service_monitor


async def cleanup_monitor():
    """모니터 정리 (앱 종료 시 호출)"""
    global _service_monitor
    
    if _service_monitor:
        await _service_monitor.stop_monitoring()
        _service_monitor = None