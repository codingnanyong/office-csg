"""
Integrated Swagger Service - 간소화된 메인 애플리케이션
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .core.config import settings
from .core.logging_config import setup_logging, get_logger
from .services.monitor import get_service_monitor, cleanup_monitor
from .services.swagger_collector import get_swagger_collector, cleanup_swagger_collector
from .api.router import api_router

# 로깅 설정
setup_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 라이프사이클 관리"""
    try:
        # 시작
        logger.info("Starting Integrated Swagger Service")
        
        # 서비스 모니터 시작
        monitor = get_service_monitor()
        await monitor.start_monitoring()
        logger.info("Service monitoring started")
        
        # Swagger 컬렉터 초기화 및 통합 스펙 생성
        swagger_collector = get_swagger_collector()
        await swagger_collector.create_integrated_spec()
        logger.info("Swagger collector initialized and integrated spec created")
        
        logger.info(f"Integrated Swagger UI available at port {settings.port}/swagger")
        
        yield
        
    finally:
        # 종료
        logger.info("Shutting down Integrated Swagger Service")
        await cleanup_monitor()
        await cleanup_swagger_collector()


# FastAPI 앱 생성 (OpenAPI 자동 생성 비활성화)
app = FastAPI(
    title="Integrated Swagger Service",
    description="통합 API 문서 서비스 - 모든 마이크로서비스의 API를 한 곳에서 확인",
    version=settings.version,
    debug=settings.debug,
    openapi_version="3.0.3",
    lifespan=lifespan,
    openapi_url=None  # 기본 OpenAPI 엔드포인트 비활성화
)

# CORS 미들웨어 추가
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 정적 파일 서빙
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# API 라우터 포함
app.include_router(api_router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main_simple:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug
    )