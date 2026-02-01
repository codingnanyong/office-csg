"""FastAPI app for sensor_mst only. Single-table container."""
from fastapi import FastAPI

from sensor_mst.router import router

app = FastAPI(title="edge-hmi sensor_mst API", version="0.1.0")
app.include_router(router)


@app.get("/")
def root():
    return {"table": "sensor_mst", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}
