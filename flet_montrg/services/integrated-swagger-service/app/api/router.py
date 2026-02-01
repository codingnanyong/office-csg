"""
API 라우터 통합 모듈
"""

from fastapi import APIRouter
from .routes import swagger, proxy, ui

# 메인 API 라우터
api_router = APIRouter()

# 각 라우터 포함
api_router.include_router(ui.router)
api_router.include_router(swagger.router)  
api_router.include_router(proxy.router)
api_router.include_router(proxy.simple_router)  # 간단한 API 프록시

__all__ = ["api_router"]