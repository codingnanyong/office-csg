"""FastAPI app for kpi_sum only (read-only). Single-table container."""
from fastapi import FastAPI

from kpi_sum.router import router

app = FastAPI(title="edge-hmi kpi_sum API", version="0.1.0")
app.include_router(router)


@app.get("/")
def root():
    return {"table": "kpi_sum", "docs": "/docs", "read_only": True}


@app.get("/health")
def health():
    return {"status": "ok"}
