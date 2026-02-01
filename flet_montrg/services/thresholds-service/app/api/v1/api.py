from fastapi import APIRouter

from app.api.v1.endpoints import thresholds

api_router = APIRouter()
api_router.include_router(thresholds.router, prefix="/thresholds", tags=["thresholds"])
