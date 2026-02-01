from fastapi import APIRouter

from app.api.v1.endpoints import realtime

api_router = APIRouter()
api_router.include_router(realtime.router, prefix="/realtime", tags=["realtime"])
