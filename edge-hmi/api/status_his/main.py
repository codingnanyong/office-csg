"""FastAPI app for status_his only (hypertable). List + Get + Create."""
from fastapi import FastAPI

from status_his.router import router

app = FastAPI(title="edge-hmi status_his API", version="0.1.0")
app.include_router(router)


@app.get("/")
def root():
    return {"table": "status_his", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}
