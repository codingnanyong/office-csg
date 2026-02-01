"""FastAPI app for worker_mst only. Single-table container."""
from fastapi import FastAPI

from worker_mst.router import router

app = FastAPI(title="edge-hmi worker_mst API", version="0.1.0")
app.include_router(router)


@app.get("/")
def root():
    return {"table": "worker_mst", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}
