import os
import time
import logging
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware
from app.api.routes import api_router
from app.core.config import settings
from app.core.logging import setup_logging

logger = logging.getLogger(__name__)


class ResponseTimeMiddleware(BaseHTTPMiddleware):
    """Request Processing Time Logging Middleware"""
    
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        # Request Processing
        response = await call_next(request)
        
        # Response Time Calculation
        process_time = time.time() - start_time
        
        # Log Recording
        logger.info(
            f"{request.client.host if request.client else 'unknown'} - "
            f'"{request.method} {request.url.path}" '
            f"{response.status_code} - "
            f"{process_time:.4f}s"
        )
        
        # Add Processing Time to Response Header (Optional)
        response.headers["X-Process-Time"] = str(process_time)
        
        return response


def create_app() -> FastAPI:
    """Create and configure the FastAPI application instance."""
    setup_logging()

    application = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        docs_url=f"{settings.api_prefix}/docs",
        redoc_url=f"{settings.api_prefix}/redoc",
        openapi_url=f"{settings.api_prefix}/openapi.json",
        debug=settings.debug,
    )

    # Add Response Time Logging Middleware
    application.add_middleware(ResponseTimeMiddleware)

    application.include_router(api_router, prefix=settings.api_prefix)

    node_name = os.getenv("NODE_NAME", "unknown")
    pod_name = os.getenv("POD_NAME", "unknown")
    pod_namespace = os.getenv("POD_NAMESPACE", "unknown")

    @application.get("/api/healthz", tags=["system"], include_in_schema=False)
    async def healthz() -> dict[str, str]:
        """Simple liveness probe endpoint that bypasses API versioning."""
        return {
            "status": "ok",
            "node": node_name,
            "pod": pod_name,
            "namespace": pod_namespace,
        }

    return application


app = create_app()


