"""FastAPI app for maint_cfg only. Single-table container."""
from fastapi import FastAPI

from maint_cfg.router import router

app = FastAPI(title="edge-hmi maint_cfg API", version="0.1.0")
app.include_router(router)


@app.get("/")
def root():
    return {"table": "maint_cfg", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}
