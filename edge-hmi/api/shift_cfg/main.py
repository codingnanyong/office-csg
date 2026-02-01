"""FastAPI app for shift_cfg only. Single-table container."""
from fastapi import FastAPI

from shift_cfg.router import router

app = FastAPI(title="edge-hmi shift_cfg API", version="0.1.0")
app.include_router(router)


@app.get("/")
def root():
    return {"table": "shift_cfg", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}
