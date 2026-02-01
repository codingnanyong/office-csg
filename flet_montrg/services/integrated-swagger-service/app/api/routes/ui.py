"""
UI 관련 라우터 - Swagger UI 및 기본 페이지
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, RedirectResponse
import logging

from ...core.config import settings
from ...services.swagger_collector import get_swagger_collector

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ui"])


@router.get("/")
async def root():
    """루트 엔드포인트 - 바로 Swagger UI 제공"""
    return FileResponse("app/static/swagger-ui.html")


@router.get("/swagger")
async def swagger_ui():
    """통합 Swagger UI 페이지 (별칭 경로)"""
    return FileResponse("app/static/swagger-ui.html")


@router.get("/info")
async def service_info():
    """서비스 정보 조회 (JSON)"""
    swagger_collector = get_swagger_collector()
    integrated_spec = swagger_collector.get_integrated_spec()
    service_count = len(integrated_spec.services) if integrated_spec else 0
    
    return {
        "service": "Integrated Swagger Service",
        "version": settings.version,
        "status": "running",
        "swagger_ui_url": "/",
        "integrated_api_docs": "/openapi.json", 
        "integrated_services_count": service_count,
        "available_endpoints": {
            "/": "Swagger UI (main)",
            "/swagger": "Swagger UI (alias)",
            "/info": "Service information",
            "/openapi.json": "OpenAPI specification",
            "/health": "Health check"
        }
    }


@router.get("/openapi.json", include_in_schema=False)
async def get_integrated_openapi_spec():
    """통합된 OpenAPI 스펙 조회 (FastAPI 기본 스펙 대신 사용)"""
    try:
        swagger_collector = get_swagger_collector()
        integrated_spec = swagger_collector.get_integrated_spec()
        
        if not integrated_spec:
            # 스펙이 없으면 새로 생성
            integrated_spec = await swagger_collector.create_integrated_spec()
        
        # 스펙이 여전히 없으면 기본 스펙 반환
        if not integrated_spec or not integrated_spec.paths:
            logger.warning("No integrated spec available, returning basic spec")
            return {
                "openapi": "3.0.3",
                "info": {
                    "title": "통합 API 문서",
                    "version": "1.0.0",
                    "description": "마이크로서비스 통합 API 문서 (스펙 수집 중...)"
                },
                "paths": {},
                "servers": [
                    {"url": f"http://localhost:{settings.port}", "description": "Integrated Swagger Service"}
                ],
                "tags": [
                    {"name": "system", "description": "시스템 관리 엔드포인트"}
                ]
            }
        
        spec_dict = integrated_spec.dict(exclude={"last_updated"})
        
        # 통합된 스펙에 서비스 목록 메타데이터 추가
        if hasattr(integrated_spec, 'services') and integrated_spec.services:
            spec_dict['x-integrated-services'] = integrated_spec.services
        
        return spec_dict
        
    except Exception as e:
        logger.error(f"Error getting integrated OpenAPI spec: {e}")
        # 오류 시 기본 스펙 반환
        return {
            "openapi": "3.0.3",
            "info": {
                "title": "통합 API 문서",
                "version": "1.0.0",
                "description": f"통합 API 문서 (오류: {str(e)})"
            },
            "paths": {},
            "servers": [{"url": f"http://localhost:{settings.port}"}]
        }


@router.get("/docs", include_in_schema=False)
async def docs_redirect():
    """FastAPI 기본 docs를 Swagger UI로 리다이렉트"""
    return RedirectResponse(url="/swagger", status_code=302)


@router.get("/dashboard", include_in_schema=False)
async def dashboard_redirect():
    """기존 대시보드 URL을 Swagger UI로 리다이렉트"""
    return RedirectResponse(url="/swagger", status_code=302)


@router.get("/health")
async def health_check():
    """헬스체크 엔드포인트"""
    return {
        "status": "healthy",
        "service": "integrated-swagger-service",
        "version": settings.version
    }