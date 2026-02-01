"""FastAPI app for line_mst only. Single-table container."""
from fastapi import FastAPI

from line_mst.router import router

app = FastAPI(title="edge-hmi line_mst API", version="0.1.0")
app.include_router(router)


@app.get("/")
def root():
    return {"table": "line_mst", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}
