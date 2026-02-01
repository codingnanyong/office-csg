"""
Swagger/OpenAPI 관련 API 라우터
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, RedirectResponse
import logging

from ...core.config import settings
from ...services.swagger_collector import get_swagger_collector

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/swagger", tags=["swagger"])


@router.get("/services")
async def get_swagger_services():
    """Swagger UI를 위한 서비스 목록 조회"""
    try:
        swagger_collector = get_swagger_collector()
        service_specs = swagger_collector.get_all_service_specs()
        
        # 서비스 스펙이 없으면 새로 수집
        if not service_specs:
            service_specs = await swagger_collector.collect_all_specs()
        
        return {"services": service_specs}
        
    except Exception as e:
        logger.error(f"Error getting swagger services: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/services/{service_name}/spec")
async def get_service_openapi_spec(service_name: str):
    """특정 서비스의 OpenAPI 스펙 조회"""
    try:
        swagger_collector = get_swagger_collector()
        service_spec = swagger_collector.get_service_spec(service_name)
        
        if not service_spec:
            raise HTTPException(status_code=404, detail=f"Service {service_name} not found")
        
        if not service_spec.is_available:
            raise HTTPException(
                status_code=503, 
                detail=f"Service {service_name} OpenAPI spec is not available: {service_spec.error_message}"
            )
        
        return service_spec.spec
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting OpenAPI spec for {service_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/refresh")
async def refresh_swagger_specs():
    """모든 서비스의 OpenAPI 스펙 새로고침"""
    try:
        swagger_collector = get_swagger_collector()
        integrated_spec = await swagger_collector.refresh_specs()
        
        return {
            "message": "OpenAPI specs refreshed successfully",
            "services_count": len(integrated_spec.services),
            "total_endpoints": len(integrated_spec.paths),
            "last_updated": integrated_spec.last_updated
        }
        
    except Exception as e:
        logger.error(f"Error refreshing swagger specs: {e}")
        raise HTTPException(status_code=500, detail=str(e))