"""FastAPI app for shift_map only. List + Get + Create."""
from fastapi import FastAPI

from shift_map.router import router

app = FastAPI(title="edge-hmi shift_map API", version="0.1.0")
app.include_router(router)


@app.get("/")
def root():
    return {"table": "shift_map", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}
