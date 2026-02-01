"""FastAPI app for kpi_cfg only. Single-table container."""
from fastapi import FastAPI

from kpi_cfg.router import router

app = FastAPI(title="edge-hmi kpi_cfg API", version="0.1.0")
app.include_router(router)


@app.get("/")
def root():
    return {"table": "kpi_cfg", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}
