from fastapi import APIRouter

from app.api.v1 import (
    banb_data,
    banbury_anomaly_detection,
    chiller_status,
    ctm_data,
    downtime,
    economin_lifespan,
    health,
    ip_data,
    ip_rollgap,
    ipi_quality_temperature,
    machine_grade,
    mc_master,
    mttr_mtbf,
    productivity,
    spare_part_inventory,
    wo_analysis,
)

api_router = APIRouter()

api_router.include_router(health.router, prefix="/v1", tags=["health"])
api_router.include_router(ip_data.router, prefix="/v1")
api_router.include_router(banb_data.router, prefix="/v1")
api_router.include_router(banbury_anomaly_detection.router, prefix="/v1")
api_router.include_router(ctm_data.router, prefix="/v1")
api_router.include_router(mc_master.router, prefix="/v1")
api_router.include_router(wo_analysis.router, prefix="/v1")
api_router.include_router(economin_lifespan.router, prefix="/v1")
api_router.include_router(machine_grade.router, prefix="/v1")
api_router.include_router(spare_part_inventory.router, prefix="/v1")
api_router.include_router(ipi_quality_temperature.router, prefix="/v1")
api_router.include_router(downtime.router, prefix="/v1")
api_router.include_router(mttr_mtbf.router, prefix="/v1")
api_router.include_router(productivity.router, prefix="/v1")
api_router.include_router(ip_rollgap.router, prefix="/v1")
api_router.include_router(chiller_status.router, prefix="/v1")


