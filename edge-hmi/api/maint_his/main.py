"""FastAPI app for maint_his only. List + Get + Create."""
from fastapi import FastAPI

from maint_his.router import router

app = FastAPI(title="edge-hmi maint_his API", version="0.1.0")
app.include_router(router)


@app.get("/")
def root():
    return {"table": "maint_his", "docs": "/docs"}


@app.get("/health")
def health():
    return {"status": "ok"}
