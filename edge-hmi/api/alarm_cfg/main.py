"""FastAPI app for alarm_cfg only. Single-table container."""
from fastapi import FastAPI

from alarm_cfg.router import router

app = FastAPI(title="edge-hmi alarm_cfg API", version="0.1.0")
app.include_router(router)


@app.get("/")
def root():
    return {"table": "alarm_cfg", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}
