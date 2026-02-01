"""FastAPI app for measurement only (hypertable). List + Create."""
from fastapi import FastAPI

from measurement.router import router

app = FastAPI(title="edge-hmi measurement API", version="0.1.0")
app.include_router(router)


@app.get("/")
def root():
    return {"table": "measurement", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}
