from fastapi import APIRouter

from app.api.v1.endpoints import mappings

api_router = APIRouter()
api_router.include_router(mappings.router, prefix="/mappings", tags=["mappings"])
