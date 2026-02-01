"""
Kubernetes client configuration and utilities
"""

import os
from typing import Optional, Dict, List
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import logging

from .config import settings

logger = logging.getLogger(__name__)


class KubernetesClient:
    """Kubernetes 클라이언트 래퍼"""
    
    def __init__(self):
        self.v1 = None
        self.apps_v1 = None
        self._setup_client()
    
    def _setup_client(self):
        """Kubernetes 클라이언트 설정"""
        try:
            # 클러스터 내부에서 실행 중인 경우
            if os.path.exists(settings.kubernetes_service_account_path):
                config.load_incluster_config()
                logger.info("Loaded in-cluster Kubernetes configuration")
            else:
                # 로컬 개발 환경
                config.load_kube_config()
                logger.info("Loaded local Kubernetes configuration")
            
            self.v1 = client.CoreV1Api()
            self.apps_v1 = client.AppsV1Api()
            
        except Exception as e:
            logger.error(f"Failed to setup Kubernetes client: {e}")
            raise
    
    async def get_services(self, namespace: str = None) -> List[Dict]:
        """네임스페이스의 서비스 목록 조회"""
        namespace = namespace or settings.kubernetes_namespace
        
        try:
            services = self.v1.list_namespaced_service(namespace=namespace)
            
            service_list = []
            for service in services.items:
                service_info = {
                    "name": service.metadata.name,
                    "namespace": service.metadata.namespace,
                    "cluster_ip": service.spec.cluster_ip,
                    "ports": [
                        {
                            "name": port.name,
                            "port": port.port,
                            "target_port": port.target_port,
                            "protocol": port.protocol
                        }
                        for port in (service.spec.ports or [])
                    ],
                    "type": service.spec.type,
                    "selector": service.spec.selector or {},
                    "created": service.metadata.creation_timestamp,
                }
                service_list.append(service_info)
            
            return service_list
            
        except ApiException as e:
            logger.error(f"Failed to get services: {e}")
            return []
    
    async def get_deployments(self, namespace: str = None) -> List[Dict]:
        """네임스페이스의 디플로이먼트 목록 조회"""
        namespace = namespace or settings.kubernetes_namespace
        
        try:
            deployments = self.apps_v1.list_namespaced_deployment(namespace=namespace)
            
            deployment_list = []
            for deployment in deployments.items:
                deployment_info = {
                    "name": deployment.metadata.name,
                    "namespace": deployment.metadata.namespace,
                    "replicas": deployment.spec.replicas,
                    "ready_replicas": deployment.status.ready_replicas or 0,
                    "available_replicas": deployment.status.available_replicas or 0,
                    "labels": deployment.metadata.labels or {},
                    "created": deployment.metadata.creation_timestamp,
                    "conditions": [
                        {
                            "type": condition.type,
                            "status": condition.status,
                            "reason": condition.reason,
                            "message": condition.message
                        }
                        for condition in (deployment.status.conditions or [])
                    ]
                }
                deployment_list.append(deployment_info)
            
            return deployment_list
            
        except ApiException as e:
            logger.error(f"Failed to get deployments: {e}")
            return []
    
    async def get_pods(self, namespace: str = None, label_selector: str = None) -> List[Dict]:
        """네임스페이스의 파드 목록 조회"""
        namespace = namespace or settings.kubernetes_namespace
        
        try:
            pods = self.v1.list_namespaced_pod(
                namespace=namespace,
                label_selector=label_selector
            )
            
            pod_list = []
            for pod in pods.items:
                pod_info = {
                    "name": pod.metadata.name,
                    "namespace": pod.metadata.namespace,
                    "phase": pod.status.phase,
                    "pod_ip": pod.status.pod_ip,
                    "node_name": pod.spec.node_name,
                    "labels": pod.metadata.labels or {},
                    "created": pod.metadata.creation_timestamp,
                    "containers": [
                        {
                            "name": container.name,
                            "image": container.image,
                            "ready": any(
                                status.name == container.name and status.ready
                                for status in (pod.status.container_statuses or [])
                            )
                        }
                        for container in pod.spec.containers
                    ]
                }
                pod_list.append(pod_info)
            
            return pod_list
            
        except ApiException as e:
            logger.error(f"Failed to get pods: {e}")
            return []
    
    async def get_service_endpoints(self, service_name: str, namespace: str = None) -> List[str]:
        """서비스의 엔드포인트 주소 목록 조회"""
        namespace = namespace or settings.kubernetes_namespace
        
        try:
            endpoints = self.v1.read_namespaced_endpoints(
                name=service_name,
                namespace=namespace
            )
            
            endpoint_list = []
            if endpoints.subsets:
                for subset in endpoints.subsets:
                    if subset.addresses:
                        for address in subset.addresses:
                            for port in (subset.ports or []):
                                endpoint_url = f"http://{address.ip}:{port.port}"
                                endpoint_list.append(endpoint_url)
            
            return endpoint_list
            
        except ApiException as e:
            logger.error(f"Failed to get service endpoints for {service_name}: {e}")
            return []
    
    def is_connected(self) -> bool:
        """Kubernetes 클러스터 연결 상태 확인"""
        try:
            self.v1.get_api_version()
            return True
        except Exception as e:
            logger.error(f"Kubernetes connection check failed: {e}")
            return False


# 전역 Kubernetes 클라이언트 인스턴스
_k8s_client: Optional[KubernetesClient] = None


def get_kubernetes_client() -> Optional[KubernetesClient]:
    """Kubernetes 클라이언트 인스턴스 반환"""
    global _k8s_client
    
    if _k8s_client is None:
        try:
            _k8s_client = KubernetesClient()
        except Exception as e:
            logger.error(f"Failed to initialize Kubernetes client: {e}")
            return None
    
    return _k8s_client