"""
API 프록시 라우터 - 마이크로서비스 API 프록시 기능
"""

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
import httpx
import logging

from ...services.discovery import get_service_discovery

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/proxy", tags=["proxy"])

# 간단한 API 라우터 (짧은 URL용)
simple_router = APIRouter(prefix="/api", tags=["simple-proxy"])


@router.api_route("/{service_name}/{path:path}", 
                  methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def api_proxy(service_name: str, path: str, request: Request):
    """마이크로서비스 API 프록시"""
    try:
        # 서비스 정보 가져오기
        discovery = get_service_discovery()
        all_services = discovery.get_all_services()
        logger.info(f"Available services: {list(all_services.keys())}")
        logger.info(f"Looking for service: {service_name}")
        
        service = discovery.get_service(service_name)
        
        # 서비스가 없으면 디스커버리 다시 실행
        if not service:
            logger.info("Service not found, running discovery again...")
            all_services = await discovery.discover_services()
            service = all_services.get(service_name)  # discover_services()의 반환값에서 직접 가져오기
            
            if not service:
                available_services = list(all_services.keys())
                raise HTTPException(
                    status_code=404, 
                    detail=f"Service {service_name} not found after discovery. Available services: {available_services}"
                )
        
        if not service.base_url:
            raise HTTPException(
                status_code=503,
                detail=f"Service {service_name} has no base URL configured"
            )
        
        # 대상 URL 구성
        clean_path = path.lstrip('/')
        target_url = f"{service.base_url.rstrip('/')}/{clean_path}"
        if request.url.query:
            target_url += f"?{request.url.query}"
        
        logger.info(f"Proxying request: {service_name} -> {target_url}")
        
        # 요청 헤더 복사 (호스트 헤더 제외)
        headers = dict(request.headers)
        headers.pop("host", None)
        
        # 요청 본문 가져오기
        body = await request.body()
        
        # HTTP 클라이언트로 실제 서비스에 요청
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(
                method=request.method,
                url=target_url,
                headers=headers,
                content=body,
            )
        
        # 응답 헤더 복사 (일부 헤더 제외)
        excluded_headers = {
            "content-encoding", "content-length", "transfer-encoding", "connection"
        }
        response_headers = {
            key: value for key, value in response.headers.items()
            if key.lower() not in excluded_headers
        }
        
        # CORS 헤더 추가
        response_headers.update({
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "*",
        })
        
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=response_headers,
            media_type=response.headers.get("content-type")
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error proxying request to {service_name}/{path}: {e}")
        raise HTTPException(status_code=500, detail=f"Proxy error: {str(e)}")


@router.options("/{service_name}/{path:path}")
async def api_proxy_options(service_name: str, path: str):
    """API 프록시 CORS preflight 요청 처리"""
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Max-Age": "3600",
        }
    )


# ======== 간단한 API 프록시 (짧은 URL) ========

@simple_router.api_route("/{resource_name}/{path:path}", 
                        methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def simple_api_proxy(resource_name: str, path: str, request: Request):
    """리소스명으로 간단한 마이크로서비스 API 프록시
    
    예: /api/thresholds/... -> thresholds-service/api/v1/thresholds/...
    """
    
    # 리소스명을 서비스명으로 매핑
    resource_to_service = {
        "thresholds": "thresholds-service",
        "location": "location-service", 
        "aggregation": "aggregation-service",
        "realtime": "realtime-service",
        "subscriptions": "alert-subscription-service",
        "alerts": "alert-service",
        "notifications": "alert-notification-service",
        "mappings": "sensor-threshold-mapping-service"
    }
    
    service_name = resource_to_service.get(resource_name)
    if not service_name:
        available_resources = list(resource_to_service.keys())
        raise HTTPException(
            status_code=404,
            detail=f"Resource '{resource_name}' not found. Available resources: {available_resources}"
        )
    
    logger.info(f"Simple proxy: {resource_name} -> {service_name}, path: /{path}")
    
    # 실제 API 경로 구성 (/api/v1/ 추가)
    full_path = f"api/v1/{resource_name}/{path}"
    
    try:
        # 서비스 정보 가져오기
        discovery = get_service_discovery()
        service = discovery.get_service(service_name)
        
        # 서비스가 없으면 디스커버리 다시 실행
        if not service:
            logger.info("Service not found, running discovery again...")
            all_services = await discovery.discover_services()
            service = all_services.get(service_name)
            
            if not service:
                available_services = list(all_services.keys())
                raise HTTPException(
                    status_code=404, 
                    detail=f"Service {service_name} not found. Available services: {available_services}"
                )
        
        if not service.base_url:
            raise HTTPException(
                status_code=503,
                detail=f"Service {service_name} has no base URL configured"
            )
        
        # 대상 URL 구성
        clean_path = full_path.lstrip('/')
        target_url = f"{service.base_url.rstrip('/')}/{clean_path}"
        if request.url.query:
            target_url += f"?{request.url.query}"
        
        logger.info(f"Simple proxying request: {resource_name} -> {target_url}")
        
        # 요청 헤더 복사 (호스트 헤더 제외)
        headers = dict(request.headers)
        headers.pop("host", None)
        
        # 요청 본문 가져오기
        body = await request.body()
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(
                method=request.method,
                url=target_url,
                headers=headers,
                content=body,
            )
            
            # 응답 헤더 필터링
            response_headers = {
                k: v for k, v in response.headers.items() 
                if k.lower() not in ["content-encoding", "content-length", "transfer-encoding"]
            }
            
            return Response(
                content=response.content,
                status_code=response.status_code,
                headers=response_headers,
                media_type=response.headers.get("content-type")
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in simple proxy to {resource_name}/{path}: {e}")
        raise HTTPException(status_code=500, detail=f"Simple proxy error: {str(e)}")


@simple_router.options("/{resource_name}/{path:path}")
async def simple_api_proxy_options(resource_name: str, path: str):
    """간단한 API 프록시 CORS preflight 요청 처리"""
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Max-Age": "3600",
        }
    )