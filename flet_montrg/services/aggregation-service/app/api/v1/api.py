from fastapi import APIRouter

from app.api.v1.endpoints import aggregation

api_router = APIRouter()
api_router.include_router(aggregation.router, prefix="/aggregation", tags=["aggregation"])
